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


package io.questdb.test.cairo.mv;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewRefreshSqlExecutionContext;
import io.questdb.cairo.mv.MatViewRefreshSqlExecutionContext;
import io.questdb.griffin.SqlCompiler;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A tick string with a date variable ($now, $today, $yesterday, $tomorrow) depends on the clock,
 * like now(). CREATE MATERIALIZED VIEW and CREATE LIVE VIEW refuse it, at the string literal, on
 * the designated timestamp (scan interval) and on any other TIMESTAMP column (IN function). The
 * refresh contexts keep compiling it, so a definition stored before CREATE refused it stays valid.
 */
public class ViewDateVariableRejectTest extends AbstractCairoTest {
    private static final String LIVE_VIEW_SELECT_PREFIX = "SELECT ts, sym, x, " +
            "sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) s FROM b2 WHERE ";

    @Test
    public void testLiveViewRefreshContextCompilesStoredDateVariable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            final LiveViewRefreshSqlExecutionContext ctx = new LiveViewRefreshSqlExecutionContext(engine, 0);
            ctx.setLiveViewCompile(true);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.compile(LIVE_VIEW_SELECT_PREFIX + "ts2 IN '$today'", ctx).getRecordCursorFactory().close();
                compiler.compile(LIVE_VIEW_SELECT_PREFIX + "ts IN '$today'", ctx).getRecordCursorFactory().close();
            } finally {
                ctx.setLiveViewCompile(false);
            }
        });
    }

    @Test
    public void testLiveViewRejectsDateVariable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            assertLiveViewRejected("ts2 IN '$today'", "'$today'");
            assertLiveViewRejected("ts IN '$today'", "'$today'");
            assertLiveViewRejected("ts2 NOT IN '$today'", "'$today'");
            assertLiveViewRejected("ts2 IN '$now - 1h..$now'", "'$now - 1h..$now'");
            assertLiveViewRejected("ts IN '$yesterday'", "'$yesterday'");
        });
    }

    @Test
    public void testMatViewAcceptsTickStringWithoutDateVariable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("""
                    INSERT INTO b2 VALUES
                      ('2026-04-01T00:10:00.000000Z', '2026-04-01T05:00:00.000000Z', 'A', 1),
                      ('2026-04-01T00:20:00.000000Z', '2026-03-31T05:00:00.000000Z', 'A', 2),
                      ('2026-04-02T00:10:00.000000Z', '2026-04-01T23:00:00.000000Z', 'A', 4)
                    """);
            execute("CREATE MATERIALIZED VIEW mvt AS SELECT ts, sym, sum(x) s FROM b2 WHERE ts2 IN '2026-04-01' SAMPLE BY 1h");
            execute("CREATE MATERIALIZED VIEW mvt2 AS SELECT ts, sym, sum(x) s FROM b2 WHERE ts IN '2026-04-01' SAMPLE BY 1h");
            drainWalAndMatViewQueues();
            // ts2 on 2026-04-01: rows x = 1 and x = 4
            assertQuery("mvt").timestamp("ts").expectSize().returns("""
                    ts\tsym\ts
                    2026-04-01T00:00:00.000000Z\tA\t1
                    2026-04-02T00:00:00.000000Z\tA\t4
                    """);
            // ts on 2026-04-01: rows x = 1 and x = 2
            assertQuery("mvt2").timestamp("ts").expectSize().returns("""
                    ts\tsym\ts
                    2026-04-01T00:00:00.000000Z\tA\t3
                    """);
        });
    }

    @Test
    public void testMatViewRefreshContextCompilesStoredDateVariable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("CREATE MATERIALIZED VIEW mvt AS SELECT ts, sym, sum(x) s FROM b2 WHERE ts2 IN '2026-04-01' SAMPLE BY 1h");
            final TableToken viewToken = engine.verifyTableName("mvt");
            try (
                    MatViewRefreshSqlExecutionContext refreshContext = new MatViewRefreshSqlExecutionContext(engine, 1);
                    TableReader baseReader = engine.getReader("b2");
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                refreshContext.of(baseReader, viewToken, false);
                compiler.compile("SELECT ts, sym, sum(x) s FROM b2 WHERE ts2 IN '$today' SAMPLE BY 1h", refreshContext)
                        .getRecordCursorFactory().close();
                compiler.compile("SELECT ts, sym, sum(x) s FROM b2 WHERE ts IN '$today' SAMPLE BY 1h", refreshContext)
                        .getRecordCursorFactory().close();
            }
        });
    }

    @Test
    public void testMatViewRejectsDateVariable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            assertMatViewRejected("ts2 IN '$today'", "'$today'");
            assertMatViewRejected("ts IN '$today'", "'$today'");
            assertMatViewRejected("ts2 NOT IN '$today'", "'$today'");
            assertMatViewRejected("ts NOT IN '$today'", "'$today'");
            assertMatViewRejected("ts2 IN '$now - 1h..$now'", "'$now - 1h..$now'");
            assertMatViewRejected("ts IN '$now - 1h..$now'", "'$now - 1h..$now'");
            assertMatViewRejected("ts2 IN '$yesterday'", "'$yesterday'");
            assertMatViewRejected("x > 0 AND ts IN '$tomorrow'", "'$tomorrow'");
        });
    }

    @Test
    public void testPlainSelectKeepsDateVariable() throws Exception {
        assertMemoryLeak(() -> {
            createBaseTable();
            execute("INSERT INTO b2 VALUES ('1970-01-01T00:00:01.000000Z', '1970-01-01T00:00:01.000000Z', 'A', 2)");
            drainWalQueue();
            // the row sits in 1970, so a real clock leaves '$today' empty for both paths
            assertQuery("SELECT x FROM b2 WHERE ts2 IN '$today'").returnsOnce("x\n");
            assertQuery("SELECT x FROM b2 WHERE ts IN '$today'").returnsOnce("x\n");
        });
    }

    private static void assertLiveViewRejected(String predicate, String literal) throws Exception {
        final String select = LIVE_VIEW_SELECT_PREFIX + predicate;
        // CREATE LIVE VIEW compiles the SELECT text on its own, so its errors carry positions
        // relative to it, as LiveViewValidationTest pins for non-deterministic functions.
        assertExceptionNoLeakCheck(
                "CREATE LIVE VIEW lvt FLUSH EVERY 1s START FROM BEGINNING AS " + select,
                select.indexOf(literal),
                "date variable cannot be used in live view"
        );
    }

    private static void assertMatViewRejected(String predicate, String literal) throws Exception {
        final String sql = "CREATE MATERIALIZED VIEW mvt AS SELECT ts, sym, sum(x) s FROM b2 WHERE " + predicate + " SAMPLE BY 1h";
        assertExceptionNoLeakCheck(sql, sql.indexOf(literal), "date variable cannot be used in materialized view");
    }

    private static void createBaseTable() throws Exception {
        execute("CREATE TABLE b2 (ts TIMESTAMP, ts2 TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }
}
