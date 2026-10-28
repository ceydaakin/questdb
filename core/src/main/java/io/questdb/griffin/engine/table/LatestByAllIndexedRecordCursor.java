/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.table;

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.idx.IndexBwdNullReader;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.AsyncQueryErrorState;
import io.questdb.cairo.sql.async.AsyncQueryProgressState;
import io.questdb.cairo.sql.async.QueryParallelFiberDispatcher;
import io.questdb.cairo.sql.async.QueryParallelOwnerLoop;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.geohash.GeoHashNative;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.Sequence;
import io.questdb.std.DirectLongList;
import io.questdb.std.Os;
import io.questdb.std.Rows;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.tasks.LatestByTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class LatestByAllIndexedRecordCursor extends AbstractPageFrameRecordCursor {
    private final int columnIndex;
    private final SOUnboundedCountDownLatch doneLatch = new SOUnboundedCountDownLatch();
    private final long indexShift = 0;
    private final QueryParallelOwnerLoop ownerLoop = new QueryParallelOwnerLoop();
    private final DirectLongList prefixes;
    private final AsyncQueryProgressState progressState = new AsyncQueryProgressState();
    private final DirectLongList rows;
    private final AsyncQueryErrorState scanError = new AsyncQueryErrorState();
    private final AtomicBooleanCircuitBreaker sharedCircuitBreaker;
    private long aIndex;
    private long aLimit;
    private long argumentsAddress;
    private MessageBus bus;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private boolean isFrameCacheBuilt;
    private boolean isTreeMapBuilt;
    private int keyCount;
    private int sharedQueryWorkerCount;

    public LatestByAllIndexedRecordCursor(
            CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull @Transient RecordMetadata metadata,
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull DirectLongList prefixes
    ) {
        super(configuration, metadata);
        sharedCircuitBreaker = new AtomicBooleanCircuitBreaker(engine);
        this.rows = rows;
        this.columnIndex = columnIndex;
        this.prefixes = prefixes;
    }

    @Override
    public void close() {
        // The shared rows list is freed here, under the per-query tracker bound in of();
        // prefixes is bounded and stays factory-owned (freed at factory close).
        rows.close();
        super.close();
    }

    @Override
    public boolean hasNext() {
        circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        if (!isTreeMapBuilt) {
            buildTreeMap();
            isTreeMapBuilt = true;
        }
        if (aIndex < aLimit) {
            // We added 1 on cpp side.
            final long rowId = rows.get(aIndex++) - 1;
            // We inverted frame indexes when posting tasks.
            final int frameIndex = Rows.MAX_SAFE_PARTITION_INDEX - Rows.toPartitionIndex(rowId);
            frameMemoryPool.navigateTo(frameIndex, recordA);
            recordA.setRowIndex(Rows.toLocalRowID(rowId));
            return true;
        }
        return false;
    }

    @Override
    public void of(PageFrameCursor pageFrameCursor, SqlExecutionContext executionContext) {
        this.frameCursor = pageFrameCursor;
        recordA.of(pageFrameCursor);
        recordB.of(pageFrameCursor);
        circuitBreaker = executionContext.getCircuitBreaker();
        bus = executionContext.getMessageBus();
        // If the worker count is 0
        sharedQueryWorkerCount = executionContext.getSharedQueryWorkerCount();
        rows.setMemoryTracker(executionContext.getMemoryTracker());
        rows.reopen();
        keyCount = -1;
        argumentsAddress = 0;
        isFrameCacheBuilt = false;
        isTreeMapBuilt = false;
        // prepare for page frame iteration
        super.init(executionContext.getMemoryTracker());
    }

    @Override
    public long preComputedStateSize() {
        return isTreeMapBuilt ? 1 : 0;
    }

    @Override
    public long size() {
        return isTreeMapBuilt ? aLimit - indexShift : -1;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async index backward scan").meta("on").putColumnName(columnIndex);
        sink.meta("workers").val(sharedQueryWorkerCount + 1);

        if (prefixes.size() > 2) {
            int geoHashColumnIndex = (int) prefixes.get(0);
            int geoHashColumnType = (int) prefixes.get(1);
            int geoHashBits = ColumnType.getGeoHashBits(geoHashColumnType);

            if (geoHashColumnIndex > -1 && ColumnType.isGeoHash(geoHashColumnType)) {
                sink.attr("filter").putColumnName(geoHashColumnIndex).val(" within(");
                for (long i = 2, n = prefixes.size(); i < n; i += 2) {
                    if (i > 2) {
                        sink.val(',');
                    }
                    sink.val(prefixes.get(i), geoHashBits);
                }
                sink.val(')');
            }
        }
    }

    @Override
    public void toTop() {
        aIndex = indexShift;
    }

    private static long getChunkSize(int keyCount, int sharedWorkerCount) {
        return sharedWorkerCount > 0 ? (keyCount + sharedWorkerCount - 1) / sharedWorkerCount : keyCount;
    }

    private static int getTaskCount(int keyCount, long chunkSize) {
        return (int) ((keyCount + chunkSize - 1) / chunkSize);
    }

    private void buildTreeMap() {
        int taskCount;
        if (keyCount < 0) {
            keyCount = getSymbolTable(columnIndex).getSymbolCount() + 1;
            final long chunkSize = getChunkSize(keyCount, sharedQueryWorkerCount);
            taskCount = getTaskCount(keyCount, chunkSize);
            rows.setCapacity(keyCount);
            GeoHashNative.iota(rows.getAddress(), rows.getCapacity(), 0);

            argumentsAddress = LatestByArguments.allocateMemoryArray(taskCount);
            for (long i = 0; i < taskCount; ++i) {
                final long keyLo = i * chunkSize;
                final long keyHi = Long.min(keyLo + chunkSize, keyCount);
                final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                LatestByArguments.setRowsAddress(argsAddress, rows.getAddress());
                LatestByArguments.setRowsCapacity(argsAddress, rows.getCapacity());
                LatestByArguments.setKeyLo(argsAddress, keyLo);
                LatestByArguments.setKeyHi(argsAddress, keyHi);
                LatestByArguments.setRowsSize(argsAddress, 0);
            }

            sharedCircuitBreaker.reset();
            scanError.clear();
        } else {
            final long chunkSize = getChunkSize(keyCount, sharedQueryWorkerCount);
            taskCount = getTaskCount(keyCount, chunkSize);
        }

        int geoHashColumnIndex = -1;
        int geoHashColumnType = ColumnType.UNDEFINED;
        long prefixesAddress = 0;
        long prefixesCount = 0;

        if (prefixes.size() > 2) {
            // Looks like we have WITHIN clause in the filter.
            geoHashColumnIndex = (int) prefixes.get(0);
            geoHashColumnType = (int) prefixes.get(1);
            prefixesAddress = prefixes.getAddress() + 2 * Long.BYTES;
            prefixesCount = prefixes.size() - 2;
        }

        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence pubSeq = bus.getLatestByPubSeq();
        final Sequence subSeq = bus.getLatestBySubSeq();
        final QueryParallelFiberDispatcher dispatcher = bus.getQueryParallelFiberDispatcher();
        ownerLoop.of(dispatcher, circuitBreaker, progressState);

        int queuedCount = 0;
        long foundRowCount = 0;
        // Once NULL has a latest row, frames without the column have nothing left to contribute.
        boolean isNullKeyFound = false;
        try {
            // First, build address cache as we'll be publishing it to other threads.
            PageFrame frame;
            if (!isFrameCacheBuilt) {
                while ((frame = frameCursor.next()) != null) {
                    frameAddressCache.add(frameCount++, frame);
                }
                isFrameCacheBuilt = true;
            }

            int frameIndex = 0;
            frameCursor.toTop();
            while ((frame = frameCursor.next()) != null && foundRowCount < keyCount) {
                final IndexReader indexReader = frame.getIndexReader(columnIndex, IndexReader.DIR_BACKWARD);
                final long indexRowLo = frame.getIndexRowLo();
                final long indexRowHi = frame.getIndexRowHi() - 1;

                if (indexReader instanceof IndexBwdNullReader) {
                    // The partition predates the column, so TableReader installs a stand-in reader with
                    // no index memory, and the native scan finds nothing in it. Every row of the frame
                    // is NULL, which makes the frame's last row NULL's latest row.
                    if (!isNullKeyFound && findNullKeyInFrameWithoutColumn(frameIndex, indexRowLo, indexRowHi, geoHashColumnIndex, geoHashColumnType, prefixesAddress, prefixesCount)) {
                        foundRowCount++;
                    }
                    // Either this frame found NULL, or an earlier frame already had.
                    isNullKeyFound = true;
                    frameIndex++;
                    continue;
                }

                final long keyBaseAddress = indexReader.getKeyBaseAddress();
                final long keysMemorySize = indexReader.getKeyMemorySize();
                final long valueBaseAddress = indexReader.getValueBaseAddress();
                final long valuesMemorySize = indexReader.getValueMemorySize();
                final int valueBlockCapacity = indexReader.getValueBlockCapacity();
                // The native fallback answers NULL's latest row as top - 1 without checking max_value, and the
                // directory-wide column top may lie beyond this frame on a composite piece or an interval-limited
                // frame, so clamp it to the frame's exclusive index row hi.
                final long unIndexedNullCount = Math.min(indexReader.getColumnTop(), frame.getIndexRowHi());

                doneLatch.reset();

                queuedCount = 0;
                ownerLoop.tryAcquirePublication();
                try {
                    for (long i = 0; i < taskCount; i++) {
                        final long argsAddress = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                        final long found = LatestByArguments.getRowsSize(argsAddress);
                        final long keyHi = LatestByArguments.getKeyHi(argsAddress);
                        final long keyLo = LatestByArguments.getKeyLo(argsAddress);

                        if (found >= keyHi - keyLo) {
                            continue;
                        }

                        final long seq = ownerLoop.hasPublication() ? pubSeq.next() : -1;
                        if (seq < 0) {
                            ownerLoop.checkBeforeHelpingNoThrottle();
                            GeoHashNative.latestByAndFilterPrefix(
                                    frameMemoryPool,
                                    keyBaseAddress,
                                    keysMemorySize,
                                    valueBaseAddress,
                                    valuesMemorySize,
                                    argsAddress,
                                    unIndexedNullCount,
                                    indexRowHi,
                                    indexRowLo,
                                    frameIndex,
                                    valueBlockCapacity,
                                    geoHashColumnIndex,
                                    geoHashColumnType,
                                    prefixesAddress,
                                    prefixesCount
                            );
                        } else {
                            queue.get(seq).of(
                                    frameAddressCache,
                                    keyBaseAddress,
                                    keysMemorySize,
                                    valueBaseAddress,
                                    valuesMemorySize,
                                    argsAddress,
                                    unIndexedNullCount,
                                    indexRowHi,
                                    indexRowLo,
                                    frameIndex,
                                    valueBlockCapacity,
                                    geoHashColumnIndex,
                                    geoHashColumnType,
                                    prefixesAddress,
                                    prefixesCount,
                                    doneLatch,
                                    sharedCircuitBreaker,
                                    progressState,
                                    scanError
                            );
                            pubSeq.done(seq);
                            queuedCount++;
                        }
                    }
                } finally {
                    ownerLoop.releasePublication();
                }

                while (true) {
                    ownerLoop.observeProgress();
                    if (doneLatch.done(queuedCount)) {
                        break;
                    }
                    if (!ownerLoop.awaitProgress()) {
                        long seq = subSeq.next();
                        if (seq > -1) {
                            runStolenTask(queue, subSeq, seq, dispatcher);
                        } else {
                            Os.pause();
                        }
                    }
                }

                foundRowCount = 0; // Reset found counter
                for (int i = 0; i < taskCount; i++) {
                    final long address = argumentsAddress + i * LatestByArguments.MEMORY_SIZE;
                    foundRowCount += LatestByArguments.getRowsSize(address);
                }

                frameIndex++;
            }
        } catch (Throwable th) {
            sharedCircuitBreaker.cancel();
            throw th;
        } finally {
            processTasks(queuedCount);
            if (sharedCircuitBreaker.checkIfTrippedOrYield()) {
                LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
                argumentsAddress = 0;
            }
        }

        if (sharedCircuitBreaker.checkIfTrippedOrYield()) {
            // A tripped shared breaker on the non-throw path means a worker scan failed, or the
            // dispatcher aborted queued tasks (quiesce); either way the row set is incomplete, so
            // the query must fail rather than return partial rows.
            if (scanError.hasError()) {
                scanError.throwError();
            }
            circuitBreaker.statefulThrowExceptionIfTrippedNoThrottleOrYield();
            throw CairoException.queryCancelled();
        }

        long rowCount = 0;
        if (argumentsAddress > 0) {
            rowCount = GeoHashNative.slideFoundBlocks(argumentsAddress, taskCount);
            LatestByArguments.releaseMemoryArray(argumentsAddress, taskCount);
            argumentsAddress = 0;
        }
        aLimit = rowCount;
        aIndex = indexShift;
        postProcessRows();
    }

    /**
     * Records NULL's latest row for a frame whose partition does not contain the column, the
     * way {@code latest_scan_backward} and {@code filter_with_prefix} would: the NULL key (0)
     * always falls into the first task's key range, found keys move to the front of the range
     * as row ids plus one, and only rows matching a WITHIN prefix count as filtered.
     */
    private boolean findNullKeyInFrameWithoutColumn(
            int frameIndex,
            long indexRowLo,
            long indexRowHi,
            int geoHashColumnIndex,
            int geoHashColumnType,
            long prefixesAddress,
            long prefixesCount
    ) {
        final long argsAddress = argumentsAddress;
        final long keyLo = LatestByArguments.getKeyLo(argsAddress);
        final long keyHi = LatestByArguments.getKeyHi(argsAddress);
        final long rowsSize = LatestByArguments.getRowsSize(argsAddress);
        final long firstNotFound = keyLo + rowsSize;
        for (long i = firstNotFound; i < keyHi; i++) {
            if (rows.get(i) == 0) {
                final long localRowId = indexRowHi - indexRowLo;
                final long rowId = Rows.toRowID(Rows.MAX_SAFE_PARTITION_INDEX - frameIndex, localRowId) + 1;
                rows.set(i, rows.get(firstNotFound));
                rows.set(firstNotFound, rowId);
                LatestByArguments.setRowsSize(argsAddress, rowsSize + 1);
                if (geoHashColumnIndex < 0 || prefixesCount == 0) {
                    LatestByArguments.setFilteredSize(argsAddress, rowsSize + 1);
                } else if (isWithinPrefix(frameIndex, localRowId, geoHashColumnIndex, geoHashColumnType, prefixesAddress, prefixesCount)) {
                    final long filteredSize = LatestByArguments.getFilteredSize(argsAddress);
                    rows.set(keyLo + filteredSize, rowId);
                    LatestByArguments.setFilteredSize(argsAddress, filteredSize + 1);
                }
                return true;
            }
        }
        return false;
    }

    private boolean isWithinPrefix(
            int frameIndex,
            long localRowId,
            int geoHashColumnIndex,
            int geoHashColumnType,
            long prefixesAddress,
            long prefixesCount
    ) {
        final long columnAddress = frameMemoryPool.navigateTo(frameIndex).getPageAddress(geoHashColumnIndex);
        if (columnAddress == 0) {
            // The geohash column is absent too, so the row's geohash is NULL and matches no prefix.
            return false;
        }
        final int size = 1 << ColumnType.pow2SizeOfBits(ColumnType.getGeoHashBits(geoHashColumnType));
        final long address = columnAddress + localRowId * size;
        final long hash = switch (size) {
            case 1 -> Unsafe.getByte(address);
            case 2 -> Unsafe.getShort(address);
            case 4 -> Unsafe.getInt(address);
            default -> Unsafe.getLong(address);
        };
        // filter_with_prefix compares in the column's own width.
        final long widthMask = size == Long.BYTES ? -1L : (1L << (size * Byte.SIZE)) - 1;
        for (long i = 0; i < prefixesCount; i += 2) {
            final long prefix = Unsafe.getLong(prefixesAddress + i * Long.BYTES);
            final long mask = Unsafe.getLong(prefixesAddress + (i + 1) * Long.BYTES);
            if ((((hash & mask) ^ prefix) & widthMask) == 0) {
                return true;
            }
        }
        return false;
    }

    private void postProcessRows() {
        Vect.sortULongAscInPlace(rows.getAddress(), aLimit);
    }

    // A stolen task publishes its own failure to its owner's error state and breaker before
    // rethrowing. Propagating a foreign task's exception here would fail this healthy query with
    // another query's error, so only an own-task failure escapes.
    private void runStolenTask(
            RingQueue<LatestByTask> queue,
            Sequence subSeq,
            long seq,
            @Nullable QueryParallelFiberDispatcher dispatcher
    ) {
        final LatestByTask task = queue.get(seq);
        final boolean isOwnTask = task.getCircuitBreaker() == sharedCircuitBreaker;
        final AsyncQueryProgressState stolenProgress = task.getProgressState();
        try {
            task.run();
        } catch (Throwable th) {
            if (isOwnTask) {
                throw th;
            }
        } finally {
            try {
                task.clear();
            } finally {
                // done(seq) releases the slot
                subSeq.done(seq);
                if (dispatcher != null) {
                    try {
                        dispatcher.signalQueueProgress();
                    } finally {
                        dispatcher.signalOwnerProgress(stolenProgress);
                    }
                }
            }
        }
    }

    private void processTasks(int queuedCount) {
        final RingQueue<LatestByTask> queue = bus.getLatestByQueue();
        final Sequence subSeq = bus.getLatestBySubSeq();
        final QueryParallelFiberDispatcher dispatcher = bus.getQueryParallelFiberDispatcher();
        while (true) {
            ownerLoop.observeProgress();
            if (doneLatch.done(queuedCount)) {
                break;
            }
            final boolean isOwnerTripped = circuitBreaker.checkIfTrippedOrYield();
            if (isOwnerTripped) {
                sharedCircuitBreaker.cancel();
            }
            if (!ownerLoop.awaitProgressWhileDraining(isOwnerTripped)) {
                long seq = subSeq.next();
                if (seq > -1) {
                    runStolenTask(queue, subSeq, seq, dispatcher);
                } else {
                    Os.pause();
                }
            }
        }
    }
}
