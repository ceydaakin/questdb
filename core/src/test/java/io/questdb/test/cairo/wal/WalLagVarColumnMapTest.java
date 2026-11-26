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

package io.questdb.test.cairo.wal;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A WAL commit that sorts a new transaction against rows held in LAG maps the LAG rows of every
 * var-size column (TableWriter.cthMergeWalVarColumnWithLag()) and, for a var-size dedup key, of the
 * key (TableWriter.deduplicateSortedIndex()). The LAG aux entries hold offsets from the start of the
 * partition's data file, so the LAG data size is the end offset of the last LAG row minus the offset
 * of the first one, not the end offset alone.
 * <p>
 * Each test fills a partition with A rows, then applies B (LAG_ROWS rows) alone, which parks it in
 * LAG, then C (NEW_ROWS rows, half a millisecond off B), which merges with the LAG. A control table
 * gets B and C in one transaction. The test checks that the LAG data mapping covers the LAG rows
 * only and that the commit grows the data file no further than the control's commit.
 */
public class WalLagVarColumnMapTest extends AbstractCairoTest {
    // The LAG data spans several append pages, so the commit maps it from the file.
    private static final long APPEND_PAGE_SIZE = Math.max(64 * 1024, Files.PAGE_SIZE);
    private static final long A_ROWS = 20_000;
    private static final long LAG_ROWS = 2_000;
    // Upper bound of the data bytes a row of any tested column takes.
    private static final long MAX_ROW_BYTES = 300;
    private static final long NEW_ROWS = 10;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, APPEND_PAGE_SIZE);
        // TableWriter parks a transaction in LAG only while LAG stays within multiplier x max uncommitted rows.
        node1.setProperty(PropertyKey.CAIRO_MAX_UNCOMMITTED_ROWS, 1_000);
        node1.setProperty(PropertyKey.CAIRO_WAL_SQUASH_UNCOMMITTED_ROWS_MULTIPLIER, 100);
        // Merge-append tables take no LAG.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        // Apply one WAL transaction per run, so B stays in LAG until C arrives.
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
    }

    @Test
    public void testArrayLagMerge() throws Exception {
        assertLagSizedByLag("DOUBLE[]", "rnd_double_array(1, 0, 0, 25)", false);
    }

    @Test
    public void testBinaryLagMerge() throws Exception {
        assertLagSizedByLag("BINARY", "rnd_bin(200, 200, 0)", false);
    }

    @Test
    public void testStringDedupKeyLagMerge() throws Exception {
        assertLagSizedByLag("STRING", "rpad(x::STRING, 100, '.')", true);
    }

    @Test
    public void testStringLagMerge() throws Exception {
        assertLagSizedByLag("STRING", "rpad(x::STRING, 100, '.')", false);
    }

    @Test
    public void testVarcharDedupKeyLagMerge() throws Exception {
        assertLagSizedByLag("VARCHAR", "rpad(x::STRING, 200, '.')::VARCHAR", true);
    }

    @Test
    public void testVarcharLagMerge() throws Exception {
        assertLagSizedByLag("VARCHAR", "rpad(x::STRING, 200, '.')::VARCHAR", false);
    }

    private static void applyOneWalTxn(TableToken token) {
        engine.getTableSequencerAPI().getTxnTracker(token).getMemPressureControl().setMaxBlockRowCount(1);
        try (ApplyWal2TableJob walApplyJob = createWalApplyJob()) {
            walApplyJob.run();
        }
    }

    private static long lagRowCount(TableToken token) {
        try (
                TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade());
                Path path = new Path()
        ) {
            txReader.ofRO(path.of(root).concat(token).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            return txReader.getLagRowCount();
        }
    }

    private static String selectA() {
        return "SELECT v, timestamp_sequence('2024-01-01T00:00:00', 1_000_000) ts FROM stage WHERE x <= " + A_ROWS;
    }

    private static String selectB() {
        return "SELECT v, timestamp_sequence('2024-01-01T06:00:00', 1_000) ts FROM stage WHERE x > " + A_ROWS + " AND x <= " + (A_ROWS + LAG_ROWS);
    }

    private static String selectC() {
        return "SELECT v, timestamp_sequence('2024-01-01T06:00:00.000500', 1_000) ts FROM stage WHERE x > " + (A_ROWS + LAG_ROWS);
    }

    private void assertLagSizedByLag(String type, String gen, boolean dedup) throws Exception {
        final ProbeFacade probe = new ProbeFacade();
        ff = probe;
        assertMemoryLeak(() -> {
            execute("CREATE TABLE stage AS (SELECT x, " + gen + " v FROM long_sequence(" + (A_ROWS + LAG_ROWS + NEW_ROWS) + "))");
            final String ddl = " (v " + type + ", ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL" + (dedup ? " DEDUP UPSERT KEYS(ts, v)" : "");
            execute("CREATE TABLE ctrl" + ddl);
            execute("CREATE TABLE t" + ddl);
            final TableToken ctrlToken = engine.verifyTableName("ctrl");
            final TableToken token = engine.verifyTableName("t");

            // Control: B and C arrive in one transaction and sort together with no LAG.
            execute("INSERT INTO ctrl " + selectA());
            applyOneWalTxn(ctrlToken);
            execute("INSERT INTO ctrl " + selectB() + " UNION ALL " + selectC());
            probe.start(ctrlToken.getDirName());
            applyOneWalTxn(ctrlToken);
            probe.stop();
            final long ctrlMaxAllocateTo = probe.maxAllocateTo.get();
            Assert.assertTrue("the control commit mapped LAG data: " + probe.lagDataMapLens, probe.lagDataMapLens.isEmpty());

            // Test: B parks in LAG, and C, which falls between B's rows, merges with it.
            execute("INSERT INTO t " + selectA());
            applyOneWalTxn(token);
            execute("INSERT INTO t " + selectB());
            execute("INSERT INTO t " + selectC());
            applyOneWalTxn(token);
            Assert.assertEquals("B must wait in LAG", LAG_ROWS, lagRowCount(token));
            probe.start(token.getDirName());
            applyOneWalTxn(token);
            probe.stop();
            final List<Long> lagDataMapLens = List.copyOf(probe.lagDataMapLens);
            final long maxAllocateTo = probe.maxAllocateTo.get();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            Assert.assertEquals(0, lagRowCount(token));
            assertSqlCursors("ctrl", "t");

            // The commit maps the LAG data, and the mapping covers the LAG rows, not the partition data before them.
            Assert.assertFalse("the commit did not map the LAG data", lagDataMapLens.isEmpty());
            final long lagBytesBound = LAG_ROWS * MAX_ROW_BYTES + APPEND_PAGE_SIZE;
            for (long len : lagDataMapLens) {
                Assert.assertTrue("LAG data mapping of " + len + " bytes for " + LAG_ROWS + " LAG rows, all mappings: " + lagDataMapLens,
                        len <= lagBytesBound);
            }
            // The commit grows the data file no further than the control's commit of the same rows.
            Assert.assertTrue("data file allocated to " + maxAllocateTo + ", the control to " + ctrlMaxAllocateTo,
                    maxAllocateTo <= ctrlMaxAllocateTo + 2 * APPEND_PAGE_SIZE);
        });
    }

    /**
     * Between start() and stop(), records the read-only MMAP_TABLE_WRITER mappings of the given table's
     * partition data file v.d (the LAG data mappings of TableWriter.mapAppendColumnBuffer()) and the
     * furthest point the file is allocated to.
     */
    private static class ProbeFacade extends TestFilesFacadeImpl {
        final List<Long> lagDataMapLens = new CopyOnWriteArrayList<>();
        final AtomicLong maxAllocateTo = new AtomicLong();
        private final ConcurrentHashMap<Long, String> fdNames = new ConcurrentHashMap<>();
        private volatile String dirName;

        @Override
        public boolean allocate(long fd, long size) {
            onAllocate(fd, size);
            return super.allocate(fd, size);
        }

        @Override
        public boolean allocate(long fd, long allocatedSize, long size) {
            onAllocate(fd, size);
            return super.allocate(fd, allocatedSize, size);
        }

        @Override
        public boolean close(long fd) {
            fdNames.remove(fd);
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (flags == Files.MAP_RO && memoryTag == MemoryTag.MMAP_TABLE_WRITER && isTracked(fd)) {
                lagDataMapLens.add(len);
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            final String path = Utf8s.stringFromUtf8Bytes(name);
            if (fd > -1 && path.endsWith(Files.SEPARATOR + "v.d") && path.contains(Files.SEPARATOR + "2024-01-01")) {
                fdNames.put(fd, path);
            }
            return fd;
        }

        private boolean isTracked(long fd) {
            final String dir = dirName;
            final String path = fdNames.get(fd);
            return dir != null && path != null && path.contains(Files.SEPARATOR + dir + Files.SEPARATOR);
        }

        private void onAllocate(long fd, long size) {
            if (isTracked(fd)) {
                maxAllocateTo.accumulateAndGet(size, Math::max);
            }
        }

        void start(String dirName) {
            lagDataMapLens.clear();
            maxAllocateTo.set(0);
            this.dirName = dirName;
        }

        void stop() {
            dirName = null;
        }
    }
}
