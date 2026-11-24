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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A designated-timestamp predicate whose bound is an expression of type NULL behaves like the same
 * predicate spelled with the {@code NULL} keyword: a comparison with NULL never holds. The interval
 * extraction used to refuse such a bound with "Invalid date", and the row filter failed on an
 * all-NULL CASE because its getters threw.
 */
public class TimestampNullExpressionBoundTest extends AbstractCairoTest {

    private static final String[] NULL_EXPRESSIONS = {
            "(CASE WHEN x > 0 THEN NULL END)",
            "(CASE WHEN true THEN NULL END)",
            "greatest(CASE WHEN x > 0 THEN NULL END)",
            "greatest(NULL)",
    };

    private static final String[] PREDICATES = {
            "ts = %s",
            "%s = ts",
            "ts != %s",
            "ts < %s",
            "ts <= %s",
            "ts > %s",
            "ts >= %s",
            "%s < ts",
            "%s >= ts",
            "ts BETWEEN %s AND '2026-05-01'",
            "ts BETWEEN '2026-03-01' AND %s",
            "ts IN (%s)",
            "ts IN (%s, '2026-04-01T00:00:00.000000Z')",
            "ts NOT IN (%s)",
    };

    @Test
    public void testComparisonWithNullExpressionReturnsNoRows() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String expr : NULL_EXPRESSIONS) {
                for (String op : new String[]{"=", "<", "<=", ">", ">="}) {
                    assertQuery("SELECT x FROM base WHERE ts " + op + " " + expr)
                            .returns("x\n");
                }
            }
        });
    }

    @Test
    public void testNullExpressionMatchesNullKeyword() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (String predicate : PREDICATES) {
                final String expected = "SELECT x FROM base WHERE " + String.format(predicate, "NULL");
                for (String expr : NULL_EXPRESSIONS) {
                    assertSqlCursors(expected, "SELECT x FROM base WHERE " + String.format(predicate, expr));
                }
            }
        });
    }

    @Test
    public void testNotInSingleTimestampFunctionMatchesNotEquals() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String value = "dateadd('d', 0, '2026-04-01T00:00:00.000000Z'::TIMESTAMP)";
            assertQuery("SELECT x FROM base WHERE ts NOT IN (" + value + ")")
                    .returns("""
                            x
                            2
                            """);
            assertSqlCursors(
                    "SELECT x FROM base WHERE ts != " + value,
                    "SELECT x FROM base WHERE ts NOT IN (" + value + ")"
            );
        });
    }

    @Test
    public void testNullCaseInRowFilterMatchesNullKeyword() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // the all-NULL CASE reaches the row filter against non-timestamp columns too
            for (String column : new String[]{"x", "sym", "ts::DOUBLE", "ts::STRING", "ts::VARCHAR"}) {
                for (String op : new String[]{"=", "!="}) {
                    assertSqlCursors(
                            "SELECT x FROM base WHERE " + column + " " + op + " NULL",
                            "SELECT x FROM base WHERE " + column + " " + op + " (CASE WHEN x > 0 THEN NULL END)"
                    );
                }
            }
        });
    }

    private static void createTable() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("""
                INSERT INTO base VALUES
                    ('2026-04-01T00:00:00.000000Z', 'A', 1),
                    ('2026-04-02T00:00:00.000000Z', 'B', 2)
                """);
        drainWalQueue();
    }
}
