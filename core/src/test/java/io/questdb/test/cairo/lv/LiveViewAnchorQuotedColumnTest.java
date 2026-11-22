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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A live view stores its ANCHOR EXPRESSION as SQL text and re-parses it at CREATE and at every
 * refresh. A column whose name needs double quotes (a space, a SQL keyword) must keep them in
 * that text, or the re-parse refuses the view or binds the anchor to a different column.
 */
public class LiveViewAnchorQuotedColumnTest extends AbstractLiveViewTest {
    // Anchored on "my col" (1, 1, 2, 2) the running sum restarts at the third row.
    private static final String MY_COL_SUMS = """
            ts\tsym\tx\ta
            2026-01-01T00:00:00.000000Z\ta\t1\t1.0
            2026-01-01T00:00:01.000000Z\ta\t2\t3.0
            2026-01-01T00:00:02.000000Z\ta\t3\t3.0
            2026-01-01T00:00:03.000000Z\ta\t4\t7.0
            """;
    // Anchored on "order" (1, 2, 2, 2) it restarts at the second row instead.
    private static final String ORDER_SUMS = """
            ts\tsym\tx\ta
            2026-01-01T00:00:00.000000Z\ta\t1\t1.0
            2026-01-01T00:00:01.000000Z\ta\t2\t2.0
            2026-01-01T00:00:02.000000Z\ta\t3\t5.0
            2026-01-01T00:00:03.000000Z\ta\t4\t9.0
            """;

    @After
    public void resetClock() {
        setCurrentMicros(-1);
    }

    @Before
    public void setUpClock() {
        setCurrentMicros(0);
    }

    @Test
    public void testAnchorExpressionOverQuotedColumn() throws Exception {
        assertAnchor("timestamp_floor('1d', ts) + \"my col\"", MY_COL_SUMS);
    }

    @Test
    public void testAnchorOnKeywordColumn() throws Exception {
        assertAnchor("\"order\"", ORDER_SUMS);
    }

    @Test
    public void testAnchorOnQuotedColumn() throws Exception {
        assertAnchor("\"my col\"", MY_COL_SUMS);
    }

    @Test
    public void testParseExpressionRejectsTrailingInput() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.parseExpression("my col");
                Assert.fail();
            } catch (SqlException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "unexpected token [col]");
                Assert.assertEquals(3, e.getPosition());
            }
        });
    }

    private void assertAnchor(String anchor, String expected) throws Exception {
        assertMemoryLeak(() -> {
            // "my" is the prefix a lost quote would leave of "my col"; it changes where "order"
            // does, so an anchor bound to it produces ORDER_SUMS instead of MY_COL_SUMS.
            execute("""
                    CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT, my LONG, "my col" LONG, "order" LONG)
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS\n"
                    + "SELECT ts, sym, x, my, \"my col\", \"order\", sum(x) OVER w a FROM base\n"
                    + "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + anchor + ")");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base (ts, sym, x, my, "my col", "order") VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1, 1, 1, 1),
                        ('2026-01-01T00:00:01.000000Z', 'a', 2, 2, 1, 2),
                        ('2026-01-01T00:00:02.000000Z', 'a', 3, 2, 2, 2),
                        ('2026-01-01T00:00:03.000000Z', 'a', 4, 2, 2, 2)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);

                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, x, a FROM lv ORDER BY ts")
                        .noLeakCheck()
                        .expectSize()
                        .timestamp("ts")
                        .returns(expected);
            }
        });
    }
}
