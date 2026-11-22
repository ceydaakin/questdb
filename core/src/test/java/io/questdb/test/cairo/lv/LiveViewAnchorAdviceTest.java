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

import org.junit.Test;

/**
 * Pins the advice of the CREATE LIVE VIEW refusals that a window call can hit while
 * the view already declares its one anchored WINDOW. A live view supports at most one
 * anchored WINDOW, so advice to add an ANCHOR or declare another anchored WINDOW
 * leads into "live view supports at most one anchored WINDOW in V1". With an
 * anchored WINDOW present, each refusal names it and the spelling that works, a
 * plain {@code OVER <name>}. Without one, each refusal keeps its ANCHOR advice.
 */
public class LiveViewAnchorAdviceTest extends AbstractLiveViewTest {

    private static final String ANCHORED_WINDOW = " FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')";
    private static final String ANCHORED_PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
            "SELECT ts, sym, sum(x) OVER w AS s, ";
    private static final String UNANCHORED_PREFIX = "CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " +
            "SELECT ts, sym, ";
    private static final String USE_W = "run the call over the view's anchored WINDOW w directly, e.g. OVER w";
    private static final String REFINES_W = "; a window that refines w with its own frame does not inherit its ANCHOR";

    private static void createBase() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
    }

    @Test
    public void testAnchoredWindowIsAcceptedThroughPlainReference() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute(ANCHORED_PREFIX + "count(x) OVER w AS c, row_number() OVER w AS rn" + ANCHORED_WINDOW);
            assertQuery("SELECT view_name FROM live_views() WHERE view_name = 'lv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .noCircuitBreakerCheck()
                    .returns("view_name\nlv\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBareUnboundedAdviceNamesAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = ANCHORED_PREFIX + "count(x) OVER (PARTITION BY sym ORDER BY ts) AS c" + ANCHORED_WINDOW;
            assertException(
                    sql,
                    sql.indexOf("sym ORDER BY ts) AS c"),
                    "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported. " +
                            "A live view supports one anchored WINDOW; " + USE_W
            );
        });
    }

    @Test
    public void testBareUnboundedAdviceWithoutAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = UNANCHORED_PREFIX + "count(x) OVER (PARTITION BY sym ORDER BY ts) AS c FROM base";
            assertException(
                    sql,
                    sql.indexOf("sym ORDER BY ts)"),
                    "live view unbounded window must have an ANCHOR clause; bare unbounded windows are not supported. " +
                            "Add an ANCHOR to bound per-partition state, e.g. ANCHOR EXPRESSION timestamp_floor('1d', ts)"
            );
        });
    }

    @Test
    public void testInlineAnchorAdviceNamesAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = ANCHORED_PREFIX + "count(x) OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00') AS c" + ANCHORED_WINDOW;
            assertException(
                    sql,
                    sql.indexOf("ANCHOR DAILY '00:00') AS c"),
                    "ANCHOR is only supported on named WINDOW clauses, and a live view supports one anchored WINDOW; " + USE_W
            );
        });
    }

    @Test
    public void testInlineAnchorAdviceWithoutAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = UNANCHORED_PREFIX + "count(x) OVER (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00') AS c FROM base";
            assertException(
                    sql,
                    sql.indexOf("ANCHOR DAILY"),
                    "ANCHOR is only supported on named WINDOW clauses; declare the window with WINDOW <name> AS (...) and reference it from the SELECT"
            );
        });
    }

    @Test
    public void testRankingAdviceNamesAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = ANCHORED_PREFIX + "row_number() OVER (PARTITION BY sym ORDER BY ts ROWS UNBOUNDED PRECEDING) AS rn" + ANCHORED_WINDOW;
            assertException(
                    sql,
                    sql.indexOf("row_number"),
                    "live view select cannot use row_number() without an anchored WINDOW; it has no finite out-of-order influence boundary. " +
                            "Instead, " + USE_W
            );
        });
    }

    @Test
    public void testRankingAdviceWithoutAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = UNANCHORED_PREFIX + "row_number() OVER (PARTITION BY sym ORDER BY ts ROWS UNBOUNDED PRECEDING) AS rn FROM base";
            assertException(
                    sql,
                    sql.indexOf("row_number"),
                    "live view select cannot use row_number() without an anchored WINDOW; it has no finite out-of-order influence boundary. " +
                            "Add an ANCHOR to reset per segment, e.g. WINDOW w AS (PARTITION BY <key> ORDER BY <ts> ANCHOR EXPRESSION timestamp_floor('1d', <ts>))"
            );
        });
    }

    @Test
    public void testUnboundedFrameAdviceNamesAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = ANCHORED_PREFIX + "count(x) OVER (PARTITION BY sym ORDER BY ts ROWS UNBOUNDED PRECEDING) AS c" + ANCHORED_WINDOW;
            assertException(
                    sql,
                    sql.indexOf("count"),
                    "live view select cannot use count() over a frame starting at UNBOUNDED PRECEDING; " +
                            "it has no finite out-of-order influence boundary, so a late row would replay the whole history. " +
                            "Bound the frame, e.g. ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW, or " + USE_W
            );
        });
    }

    @Test
    public void testUnboundedFrameAdviceSaysRefinementDropsAnchor() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = ANCHORED_PREFIX + "count(x) OVER (w RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS c" + ANCHORED_WINDOW;
            assertException(
                    sql,
                    sql.indexOf("count"),
                    "live view select cannot use count() over a frame starting at UNBOUNDED PRECEDING; " +
                            "it has no finite out-of-order influence boundary, so a late row would replay the whole history. " +
                            "Bound the frame, e.g. ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW, or " + USE_W + REFINES_W
            );
        });
    }

    @Test
    public void testUnboundedFrameAdviceWithoutAnchoredWindow() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            final String sql = UNANCHORED_PREFIX + "count(x) OVER (PARTITION BY sym ORDER BY ts ROWS UNBOUNDED PRECEDING) AS c FROM base";
            assertException(
                    sql,
                    sql.indexOf("count"),
                    "live view select cannot use count() over a frame starting at UNBOUNDED PRECEDING; " +
                            "it has no finite out-of-order influence boundary, so a late row would replay the whole history. " +
                            "Bound the frame, e.g. ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW, or add an ANCHOR to reset per segment, " +
                            "e.g. WINDOW w AS (PARTITION BY <key> ORDER BY <ts> ANCHOR EXPRESSION timestamp_floor('1d', <ts>))"
            );
        });
    }
}
