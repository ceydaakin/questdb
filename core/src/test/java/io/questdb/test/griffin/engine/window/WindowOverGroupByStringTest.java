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

package io.questdb.test.griffin.engine.window;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Ranking window functions and ORDER BY read two rows of a GROUP BY result at
 * once, one through getStrA()/getVarcharA() and one through the B getter.
 * STRING and VARCHAR aggregates must return independent views from them.
 */
public class WindowOverGroupByStringTest extends AbstractCairoTest {
    private static final String[] AGGREGATES = {
            "min(s)", "max(s)", "first(s)", "last(s)", "first_not_null(s)", "last_not_null(s)",
            "min(v)", "max(v)", "first(v)", "last(v)", "first_not_null(v)", "last_not_null(v)",
            "arg_max(v, d)", "arg_max(v, i)", "arg_max(v, l)", "arg_max(v, ts)",
    };

    @Test
    public void testOrderByAggregateWithoutEncodeSort() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTable();
            for (String agg : AGGREGATES) {
                assertQuery("SELECT k, " + agg + " m FROM t GROUP BY k ORDER BY m")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tm
                                k4\ta
                                k2\tb
                                k1\td
                                k3\te
                                """);
            }
        });
    }

    @Test
    public void testRankOverAggregate() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String agg : AGGREGATES) {
                assertQuery("SELECT k, m, rank() OVER (ORDER BY m) r, dense_rank() OVER (ORDER BY m) dr, " +
                        "percent_rank() OVER (ORDER BY m) pr, cume_dist() OVER (ORDER BY m) cd " +
                        "FROM (SELECT k, " + agg + " m FROM t GROUP BY k) ORDER BY k")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tm\tr\tdr\tpr\tcd
                                k1\td\t3\t3\t0.6666666666666666\t0.75
                                k2\tb\t2\t2\t0.3333333333333333\t0.5
                                k3\te\t4\t4\t1.0\t1.0
                                k4\ta\t1\t1\t0.0\t0.25
                                """);
            }
        });
    }

    @Test
    public void testRankOverAggregatePartitioned() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String agg : AGGREGATES) {
                assertQuery("SELECT k, m, rank() OVER (PARTITION BY p ORDER BY m) r, " +
                        "dense_rank() OVER (PARTITION BY p ORDER BY m) dr, " +
                        "percent_rank() OVER (PARTITION BY p ORDER BY m) pr, " +
                        "cume_dist() OVER (PARTITION BY p ORDER BY m) cd " +
                        "FROM (SELECT k, p, " + agg + " m FROM t GROUP BY k, p) ORDER BY k")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                k\tm\tr\tdr\tpr\tcd
                                k1\td\t2\t2\t1.0\t1.0
                                k2\tb\t1\t1\t0.0\t0.5
                                k3\te\t2\t2\t1.0\t1.0
                                k4\ta\t1\t1\t0.0\t0.5
                                """);
            }
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE t (k SYMBOL, p SYMBOL, s STRING, v VARCHAR, d DOUBLE, i INT, l LONG, ts TIMESTAMP) " +
                "TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                    ('k1', 'x', 'd', 'd', 1.0, 1, 1, '2024-01-01T01'),
                    ('k2', 'x', 'b', 'b', 1.0, 1, 1, '2024-01-01T02'),
                    ('k3', 'y', 'e', 'e', 1.0, 1, 1, '2024-01-02T01'),
                    ('k4', 'y', 'a', 'a', 1.0, 1, 1, '2024-01-03T01')
                """);
    }
}
