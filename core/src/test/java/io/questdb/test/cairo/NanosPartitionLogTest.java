/*******************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * Log lines that name a partition must format a TIMESTAMP_NS partition timestamp as
 * nanoseconds, not read it as microseconds (which lands around the year 57971).
 */
public class NanosPartitionLogTest extends AbstractCairoTest {
    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
    }

    @Test
    public void testConvertParquetToNativeBeforeTypeChange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tn (ts TIMESTAMP_NS, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO tn VALUES
                    ('2026-01-01T00:00:00.000000000Z', 'a'),
                    ('2026-01-02T00:00:00.000000000Z', 'b')
                    """);
            execute("ALTER TABLE tn CONVERT PARTITION TO PARQUET LIST '2026-01-01'");
            execute("ALTER TABLE tn ALTER COLUMN v TYPE SYMBOL");
            capture.drain();
            capture.assertLogged("converting parquet partition to native before type change"
                    + " [partition=2026-01-01T00:00:00.000000000Z, column=v");
        });
    }

    @Test
    public void testSplitLastLineOfPartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1_000);
        node1.setProperty(PropertyKey.CAIRO_O3_LAST_PARTITION_MAX_SPLITS, 1_000);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                    "SELECT x::INT i, timestamp_sequence('2024-01-01', 1_000_000L)::TIMESTAMP_NS ts" +
                    " FROM long_sequence(20_000)) TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            // an O3 insert in the middle of the day splits it in two
            execute("INSERT INTO x (i, ts) SELECT x::INT + 200_000," +
                    " timestamp_sequence('2024-01-01T05:00:00', 1_000L)::TIMESTAMP_NS FROM long_sequence(200)");
            drainWalQueue();

            final long siblingTs;
            try (TableReader reader = engine.getReader(engine.verifyTableName("x"))) {
                final TxReader txFile = reader.getTxFile();
                Assert.assertEquals("fixture did not split the day in two", 2, txFile.getPartitionCount());
                siblingTs = txFile.getPartitionTimestampByIndex(1);
            }

            // Replace everything from the sibling's floor upwards: the sibling loses all its rows,
            // so the writer splits the last line of the parent partition into a new split.
            try (WalWriter ww = engine.getWalWriter(engine.verifyTableName("x"))) {
                final TableWriter.Row row = ww.newRow(NanosTimestampDriver.floor("2024-01-09T00:00:00.000000000Z"));
                row.putInt(0, 999);
                row.append();
                ww.commitWithParams(siblingTs, Long.MAX_VALUE, WAL_DEDUP_MODE_REPLACE_RANGE);
            }
            drainWalQueue();

            capture.drain();
            capture.assertLogged("splitting last line of the partition [table=");
            capture.assertLogged(", partition=2024-01-01T00:00:00.000000000Z, oldSize=");
        });
    }
}
