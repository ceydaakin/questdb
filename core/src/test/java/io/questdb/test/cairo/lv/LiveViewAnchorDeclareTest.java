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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A live view's {@code ANCHOR EXPRESSION} must substitute {@code DECLARE} variables the same
 * way the rest of the query does. The anchor is stored as text and re-parsed at CREATE and at
 * refresh without the {@code DECLARE} block, so a variable left in the stored text reads as a
 * column name.
 */
public class LiveViewAnchorDeclareTest extends AbstractLiveViewTest {
    private static final String EXPECTED = """
            ts\tsym\tx\ts
            2026-01-01T00:00:00.000000Z\ta\t1\t1.0
            2026-01-01T00:30:00.000000Z\ta\t2\t3.0
            2026-01-01T01:00:00.000000Z\ta\t3\t3.0
            2026-01-01T01:30:00.000000Z\ta\t4\t7.0
            2026-01-01T00:00:00.000000Z\tb\t10\t10.0
            2026-01-01T00:30:00.000000Z\tb\t20\t30.0
            2026-01-01T01:00:00.000000Z\tb\t30\t30.0
            2026-01-01T01:30:00.000000Z\tb\t40\t70.0
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
    public void testAnchorExpressionIsDeclaredVariable() throws Exception {
        assertAnchoredSums("""
                CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                DECLARE @a := timestamp_floor('1h', ts)
                SELECT ts, sym, x, sum(x) OVER w AS s FROM base
                WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION @a)""");
    }

    @Test
    public void testAnchorExpressionInlineControl() throws Exception {
        assertAnchoredSums("""
                CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                SELECT ts, sym, x, sum(x) OVER w AS s FROM base
                WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))""");
    }

    @Test
    public void testAnchorExpressionUsesDeclaredVariable() throws Exception {
        assertAnchoredSums("""
                CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                DECLARE @stride := '1h'
                SELECT ts, sym, x, sum(x) OVER w AS s FROM base
                WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor(@stride, ts))""");
    }

    @Test
    public void testDeclaredConstantAnchorIsRejectedAsConstant() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertExceptionNoLeakCheck(
                    """
                            CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                            DECLARE @a := 5
                            SELECT ts, sym, x, sum(x) OVER w AS s FROM base
                            WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION @a)""",
                    70,
                    "ANCHOR EXPRESSION must not be a constant"
            );
        });
    }

    @Test
    public void testUndeclaredVariableInAnchorExpressionIsRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            assertExceptionNoLeakCheck(
                    """
                            CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS
                            DECLARE @a := '1h'
                            SELECT ts, sym, x, sum(x) OVER w AS s FROM base
                            WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor(@b, ts))""",
                    199,
                    "tried to use undeclared variable `@b`"
            );
        });
    }

    private void assertAnchoredSums(String createSql) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(createSql);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("""
                        INSERT INTO base (ts, sym, x) VALUES
                        ('2026-01-01T00:00:00.000000Z', 'a', 1),
                        ('2026-01-01T00:00:00.000000Z', 'b', 10),
                        ('2026-01-01T00:30:00.000000Z', 'a', 2),
                        ('2026-01-01T00:30:00.000000Z', 'b', 20),
                        ('2026-01-01T01:00:00.000000Z', 'a', 3),
                        ('2026-01-01T01:00:00.000000Z', 'b', 30),
                        ('2026-01-01T01:30:00.000000Z', 'a', 4),
                        ('2026-01-01T01:30:00.000000Z', 'b', 40)""");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                driveRefreshToQuiescence(job);

                assertNoRefreshFaults("lv");
                assertQuery("SELECT ts, sym, x, s FROM lv ORDER BY sym, ts")
                        .noLeakCheck()
                        .expectSize()
                        .returns(EXPECTED);
            }
        });
    }
}
