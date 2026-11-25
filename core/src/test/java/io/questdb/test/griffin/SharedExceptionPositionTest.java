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


package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.griffin.engine.LimitOverflowException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * CREATE TABLE AS SELECT, CREATE VIEW and CREATE MATERIALIZED VIEW write their name's
 * position into the CairoException that fails them. Pooled exception objects must not
 * carry that position into later, unrelated errors.
 */
public class SharedExceptionPositionTest extends AbstractCairoTest {

    @Test
    public void testEntryLockedInstanceResetsPosition() {
        EntryLockedException.instance("first").position(13);
        Assert.assertEquals(0, EntryLockedException.instance("second").getPosition());
    }

    @Test
    public void testFailedCreateTableAsDoesNotLeakPositionToSelect() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_MAP_MAX_RESIZES, 0);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_KEY_CAPACITY, 32);
        setProperty(PropertyKey.CAIRO_SQL_SMALL_MAP_PAGE_SIZE, 32 * 1024);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE base AS (
                        SELECT ('a' || (x % 100))::SYMBOL s, ('b' || (x / 100))::SYMBOL t, x v, (x * 1_000_000L)::TIMESTAMP ts
                        FROM long_sequence(10_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY""");
            assertExceptionNoLeakCheck(
                    "CREATE TABLE t AS (SELECT v FROM base LATEST ON ts PARTITION BY s, t)",
                    13,
                    "limit of 0 resizes exceeded"
            );
            assertExceptionNoLeakCheck(
                    "SELECT v FROM base LATEST ON ts PARTITION BY s, t",
                    0,
                    "limit of 0 resizes exceeded"
            );
        });
    }

    @Test
    public void testLimitOverflowInstanceResetsPosition() {
        LimitOverflowException.instance().put("first").position(13);
        LimitOverflowException ex = LimitOverflowException.instance();
        Assert.assertEquals(0, ex.getPosition());
        Assert.assertEquals(0, ex.getFlyweightMessage().length());
    }

    @Test
    public void testPoolClosedExceptionIsNotShared() {
        PoolClosedException first = PoolClosedException.instance();
        first.position(13);
        PoolClosedException second = PoolClosedException.instance();
        Assert.assertNotSame(first, second);
        Assert.assertEquals(0, second.getPosition());
    }
}
