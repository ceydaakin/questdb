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

package io.questdb.test.griffin.engine.groupby;

import io.questdb.PropertyKey;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * SAMPLE BY first/last with {@code WHERE s = NULL} over a BITMAP-indexed symbol, which
 * runs the index-backed SampleByFirstLast factory. Every scenario builds two identical
 * tables, {@code t_idx} with {@code s SYMBOL INDEX} and {@code t_plain} with an unindexed
 * {@code s}. The unindexed twin takes the ordinary SAMPLE BY plan and checks the
 * hand-computed expectation; the indexed table must then return the same rows.
 */
public class SampleByFirstLastNullKeyTest extends AbstractCairoTest {

    private static final String QUERY = "SELECT k, first(lat), last(lon) FROM %s WHERE s = %s SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";

    @Test
    public void testColumnTop() throws Exception {
        // Rows 0 and 1 predate s and read as NULL; rows 2 and 3 carry 'a'.
        assertMemoryLeak(() -> {
            createTables(
                    "CREATE TABLE %1$s (lat DOUBLE, lon DOUBLE, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY BYPASS WAL",
                    "INSERT INTO %1$s VALUES (1, 1, '1970-01-01T00:00'), (2, 2, '1970-01-01T00:10')",
                    "ALTER TABLE %1$s ADD COLUMN s SYMBOL%2$s",
                    "INSERT INTO %1$s VALUES (3, 3, '1970-01-01T00:20', 'a'), (4, 4, '1970-01-01T01:30', 'a')"
            );
            assertBoth("NULL", """
                    k\tfirst\tlast
                    1970-01-01T00:00:00.000000Z\t1.0\t2.0
                    """);
        });
    }

    @Test
    public void testColumnTopLongerThanOutputBuffer() throws Exception {
        // The smallest search page holds 15 output rows. A 40-row NULL prefix with one row per
        // bucket fills it twice, so the search has to resume inside the prefix instead of
        // moving on to the next page frame. Row 40 is an explicit NULL above the top.
        setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_PAGE_SIZE, 16);
        assertMemoryLeak(() -> {
            createTables(
                    "CREATE TABLE %1$s (lat DOUBLE, lon DOUBLE, k TIMESTAMP) TIMESTAMP(k) PARTITION BY YEAR BYPASS WAL",
                    "INSERT INTO %1$s SELECT x, x, ((x - 1) * 3_600_000_000L)::TIMESTAMP FROM long_sequence(40)",
                    "ALTER TABLE %1$s ADD COLUMN s SYMBOL%2$s",
                    """
                            INSERT INTO %1$s VALUES
                            (41, 41, '1970-01-02T16:00', 'a'), (42, 42, '1970-01-02T17:00', NULL),
                            (43, 43, '1970-01-02T18:00', 'a')
                            """
            );
            final StringSink expected = new StringSink();
            expected.put("k\tfirst\tlast\n");
            for (int hour = 0; hour < 40; hour++) {
                appendRow(expected, hour, hour + 1);
            }
            appendRow(expected, 41, 42);
            assertBoth("NULL", expected.toString());
        });
    }

    @Test
    public void testMultiPartitionColumnTop() throws Exception {
        // 1970-01-01 predates s entirely, 1970-01-02 has a top of 2 plus one explicit NULL
        // followed by an 'a', 1970-01-03 is written after s exists and holds no NULL.
        assertMemoryLeak(() -> {
            createTables(
                    "CREATE TABLE %1$s (lat DOUBLE, lon DOUBLE, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY BYPASS WAL",
                    """
                            INSERT INTO %1$s VALUES
                            (1, 1, '1970-01-01T00:00'), (2, 2, '1970-01-01T00:10'),
                            (3, 3, '1970-01-02T00:00'), (4, 4, '1970-01-02T00:10')
                            """,
                    "ALTER TABLE %1$s ADD COLUMN s SYMBOL%2$s",
                    """
                            INSERT INTO %1$s VALUES
                            (5, 5, '1970-01-02T00:20', 'a'), (6, 6, '1970-01-02T00:30', NULL),
                            (7, 7, '1970-01-02T00:40', 'a'), (8, 8, '1970-01-03T00:00', 'a'),
                            (9, 9, '1970-01-03T01:00', 'a')
                            """
            );
            // Control for a non-NULL key: buckets align to the first 'a' at 01-02T00:20.
            assertBoth("'a'", """
                    k\tfirst\tlast
                    1970-01-02T00:20:00.000000Z\t5.0\t7.0
                    1970-01-02T23:20:00.000000Z\t8.0\t8.0
                    1970-01-03T00:20:00.000000Z\t9.0\t9.0
                    """);
            // NULL rows: 1, 2 (absent column), 3, 4 (below the top), 6 (explicit NULL).
            assertBoth("NULL", """
                    k\tfirst\tlast
                    1970-01-01T00:00:00.000000Z\t1.0\t2.0
                    1970-01-02T00:00:00.000000Z\t3.0\t6.0
                    """);
        });
    }

    @Test
    public void testNoNullsAtAll() throws Exception {
        assertMemoryLeak(() -> {
            createTables(
                    "CREATE TABLE %1$s (lat DOUBLE, lon DOUBLE, s SYMBOL%2$s, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY BYPASS WAL",
                    "INSERT INTO %1$s VALUES (1, 1, 'a', '1970-01-01T00:00'), (2, 2, 'a', '1970-01-01T00:10'), (3, 3, 'a', '1970-01-01T01:30')"
            );
            assertBoth("NULL", "k\tfirst\tlast\n");
        });
    }

    @Test
    public void testNullKeyEntriesUsedUp() throws Exception {
        // The NULL key's only entry is row 1, the key 'a' keeps rows after it in the same frame.
        assertMemoryLeak(() -> {
            createTables(
                    "CREATE TABLE %1$s (lat DOUBLE, lon DOUBLE, s SYMBOL%2$s, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY BYPASS WAL",
                    """
                            INSERT INTO %1$s VALUES
                            (1, 1, 'a', '1970-01-01T00:00'), (2, 2, NULL, '1970-01-01T00:10'),
                            (3, 3, 'a', '1970-01-01T00:20'), (4, 4, 'a', '1970-01-01T01:30')
                            """
            );
            // Control for a non-NULL key: rows 0, 2 in the 00:00 bucket, row 3 in 01:00.
            assertBoth("'a'", """
                    k\tfirst\tlast
                    1970-01-01T00:00:00.000000Z\t1.0\t3.0
                    1970-01-01T01:00:00.000000Z\t4.0\t4.0
                    """);
            // The only NULL row is row 1 at 00:10; first observation aligns the bucket there.
            assertBoth("NULL", """
                    k\tfirst\tlast
                    1970-01-01T00:10:00.000000Z\t2.0\t2.0
                    """);
        });
    }

    @Test
    public void testSymbolAbsentFromWholeTable() throws Exception {
        // Every partition predates s, so each one reads through the stand-in null reader
        // and every row matches the NULL key; none matches 'a'.
        assertMemoryLeak(() -> {
            createTables(
                    "CREATE TABLE %1$s (lat DOUBLE, lon DOUBLE, k TIMESTAMP) TIMESTAMP(k) PARTITION BY DAY BYPASS WAL",
                    "INSERT INTO %1$s VALUES (1, 1, '1970-01-01T00:00'), (2, 2, '1970-01-01T00:10'), (3, 3, '1970-01-02T01:00')",
                    "ALTER TABLE %1$s ADD COLUMN s SYMBOL%2$s"
            );
            assertBoth("NULL", """
                    k\tfirst\tlast
                    1970-01-01T00:00:00.000000Z\t1.0\t2.0
                    1970-01-02T01:00:00.000000Z\t3.0\t3.0
                    """);
            assertBoth("'a'", "k\tfirst\tlast\n");
        });
    }

    private static void appendRow(StringSink sink, int hour, int value) {
        sink.put("1970-01-0").put(1 + hour / 24).put('T');
        final int hh = hour % 24;
        if (hh < 10) {
            sink.put('0');
        }
        sink.put(hh).put(":00:00.000000Z\t").put(value).put(".0\t").put(value).put(".0\n");
    }

    private static void createTables(String... statements) throws Exception {
        for (String sql : statements) {
            execute(sql.formatted("t_idx", " INDEX"));
            execute(sql.formatted("t_plain", ""));
        }
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    private void assertBoth(String key, String expected) throws Exception {
        assertQuery(QUERY.formatted("t_plain", key))
                .noLeakCheck()
                .noRandomAccess()
                .sizeMayVary()
                .timestamp("k")
                .withPlanNotContaining("SampleByFirstLast")
                .returns(expected);
        assertQuery(QUERY.formatted("t_idx", key))
                .noLeakCheck()
                .noRandomAccess()
                .sizeMayVary()
                .timestamp("k")
                .withPlanContaining("SampleByFirstLast")
                .returns(expected);
    }
}
