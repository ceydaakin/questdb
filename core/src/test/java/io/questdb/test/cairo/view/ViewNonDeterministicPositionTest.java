/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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


package io.questdb.test.cairo.view;

import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A materialized or live view that reads a view whose body has a non-deterministic function, or a
 * sub-query over an external source, is refused at the name of that view in the compiled SQL, the
 * same way an execution requirement of a function written in the view is reported. The function's
 * own position points into the view's stored text, not into the statement.
 */
public class ViewNonDeterministicPositionTest extends AbstractCairoTest {

    @Test
    public void testLiveViewReportsViewPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTablesAndRndView();
            final String selectSql = "SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) s\n" +
                    "FROM base WHERE sym IN (SELECT sym FROM v_rnd)";
            // CREATE LIVE VIEW compiles its SELECT text on its own, so positions of errors in the body are
            // relative to that text, see LiveViewValidationTest.assertLiveViewCreateRejected().
            assertQuery("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM BEGINNING AS\n" + selectSql)
                    .noLeakCheck()
                    .fails(selectSql.indexOf("v_rnd"), "non-deterministic function cannot be used in live view: rnd_int");
        });
    }

    @Test
    public void testMatViewDirectFunctionKeepsOwnPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTablesAndRndView();
            final String sql = "CREATE MATERIALIZED VIEW mv AS SELECT ts, sym, sum(x) s FROM base " +
                    "WHERE sym IN (SELECT sym FROM other WHERE x > rnd_int()) SAMPLE BY 1h";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("rnd_int"), "non-deterministic function cannot be used in materialized view: rnd_int");
        });
    }

    @Test
    public void testMatViewExternalSubQueryReportsViewPosition() throws Exception {
        // read_parquet() reads only from sql.copy.input.root
        inputRoot = root;
        try {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE ext AS (SELECT '2024-01-01T00:00:00.000000Z'::TIMESTAMP AS value FROM long_sequence(3))");
                try (
                        Path path = new Path();
                        PartitionDescriptor descriptor = new PartitionDescriptor();
                        TableReader reader = engine.getReader("ext")
                ) {
                    path.of(root).concat("ext.parquet");
                    PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                    PartitionEncoder.encode(descriptor, path);
                }
                createTablesAndRndView();
                execute("CREATE VIEW v_ext AS (SELECT sym FROM other WHERE ts > (SELECT max(value) FROM read_parquet('ext.parquet')))");
                drainWalAndViewQueues();
                final String sql = "CREATE MATERIALIZED VIEW mv AS SELECT ts, sym, sum(x) s FROM base " +
                        "WHERE sym IN (SELECT sym FROM v_ext) SAMPLE BY 1h";
                assertQuery(sql)
                        .noLeakCheck()
                        .fails(sql.indexOf("v_ext"), "non-deterministic function cannot be used in materialized view: sub-query");
            });
        } finally {
            inputRoot = null;
        }
    }

    @Test
    public void testMatViewReportsViewPosition() throws Exception {
        assertMemoryLeak(() -> {
            createTablesAndRndView();
            final String sql = "CREATE MATERIALIZED VIEW mv AS SELECT ts, sym, sum(x) s FROM base " +
                    "WHERE sym IN (SELECT sym FROM v_rnd) SAMPLE BY 1h";
            assertQuery(sql)
                    .noLeakCheck()
                    .fails(sql.indexOf("v_rnd"), "non-deterministic function cannot be used in materialized view: rnd_int");
        });
    }

    private static void createTablesAndRndView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE TABLE other (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE VIEW v_rnd AS (SELECT sym FROM other WHERE x > rnd_int())");
        drainWalAndViewQueues();
    }
}
