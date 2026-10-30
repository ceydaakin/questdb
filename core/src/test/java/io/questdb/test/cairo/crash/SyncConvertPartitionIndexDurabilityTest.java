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


package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableToken;
import org.junit.Test;

/**
 * Under cairo.commit.mode=sync, CONVERT PARTITION TO PARQUET must flush the index files it rebuilds before the
 * _txn that publishes the converted partition. The BITMAP-indexed column s has a column top on 2024-01-01, so the
 * conversion rebuilds its index rather than hard-linking the native one.
 */
public class SyncConvertPartitionIndexDurabilityTest extends AbstractAdaptiveCrashTest {

    @Test
    public void testNonWal() throws Exception {
        assertSurvivesPowerLoss(false);
    }

    @Test
    public void testWal() throws Exception {
        assertSurvivesPowerLoss(true);
    }

    private void assertSurvivesPowerLoss(boolean wal) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY " + (wal ? "WAL" : "BYPASS WAL"));
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 0.5)");
            drainWalQueue();
            execute("ALTER TABLE x ADD COLUMN s SYMBOL INDEX");
            drainWalQueue();
            execute("""
                    INSERT INTO x (ts, v, s) VALUES
                        ('2024-01-01T01:00:00.000000Z', 1.0, 'a'),
                        ('2024-01-01T02:00:00.000000Z', 1.5, 'a'),
                        ('2024-01-02T00:00:00.000000Z', 2.0, 'b'),
                        ('2024-01-03T00:00:00.000000Z', 3.0, 'a')
                    """);
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            drainWalQueue();
            final String query = "SELECT ts, s FROM x WHERE s = 'a'";
            final String expected = """
                    ts\ts
                    2024-01-01T01:00:00.000000Z\ta
                    2024-01-01T02:00:00.000000Z\ta
                    2024-01-03T00:00:00.000000Z\ta
                    """;
            assertQuery(query).timestamp("ts").withPlanContaining("Index forward scan on: s").returns(expected);

            if (wal) {
                recoverAfterCrash(new TableToken[]{token});
            } else {
                crashAndReopen();
            }
            assertQuery(query).timestamp("ts").withPlanContaining("Index forward scan on: s").returns(expected);
            releaseEngineHandles();
        });
    }
}
