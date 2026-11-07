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

package io.questdb.test.griffin.engine.functions.rnd;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A random symbol function must give one key to each distinct value, even when its
 * list repeats a value, otherwise key-based operators count a value twice.
 */
public class RndSymbolRepeatedValuesTest extends AbstractCairoTest {

    @Test
    public void testCountDistinctGeneratedValues() throws Exception {
        // rnd_symbol(50, 1, 1, 0) draws 50 one-letter values, so at most 26 are distinct.
        assertMemoryLeak(() -> assertQuery(
                "SELECT x % 1 k, count_distinct(rnd_symbol(50, 1, 1, 0)) <= 26 c FROM long_sequence(1000)"
        ).expectSize().returns("""
                k\tc
                0\ttrue
                """));
    }

    @Test
    public void testCountDistinctList() throws Exception {
        assertMemoryLeak(() -> {
            assertCountDistinct("rnd_symbol('a', 'a', 'b')");
            assertCountDistinct("rnd_symbol_weighted('a', 1, 'a', 1, 'b', 1)");
            assertCountDistinct("rnd_symbol_zipf('a', 'a', 'b', 0.5)");
        });
    }

    @Test
    public void testCountDistinctListWithCount() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "SELECT count_distinct(rnd_symbol('a', 'a', 'b')) c, count() n FROM long_sequence(100)"
        ).noRandomAccess().expectSize().returns("""
                c\tn
                2\t100
                """));
    }

    @Test
    public void testDistinctSubquery() throws Exception {
        assertMemoryLeak(() -> {
            assertDistinct("rnd_symbol('a', 'a', 'b')");
            assertDistinct("rnd_symbol_weighted('a', 1, 'a', 1, 'b', 1)");
            assertDistinct("rnd_symbol_zipf('a', 'a', 'b', 0.5)");
        });
    }

    @Test
    public void testGroupBySubquery() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "SELECT s, count() > 0 n FROM (SELECT rnd_symbol('a', 'a', 'b') s FROM long_sequence(100)) GROUP BY s ORDER BY s"
        ).expectSize().returns("""
                s\tn
                a\ttrue
                b\ttrue
                """));
    }

    @Test
    public void testStringDistinctAgg() throws Exception {
        assertMemoryLeak(() -> assertQuery(
                "SELECT length(string_distinct_agg(rnd_symbol('a', 'a', 'b'), ',')) l, count() n FROM long_sequence(100)"
        ).noRandomAccess().expectSize().returns("""
                l\tn
                3\t100
                """));
    }

    private void assertCountDistinct(String function) throws Exception {
        assertQuery("SELECT x % 1 k, count_distinct(" + function + ") c FROM long_sequence(100)")
                .expectSize()
                .returns("""
                        k\tc
                        0\t2
                        """);
    }

    private void assertDistinct(String function) throws Exception {
        assertQuery("SELECT DISTINCT s FROM (SELECT " + function + " s FROM long_sequence(100)) ORDER BY s")
                .expectSize()
                .returns("""
                        s
                        a
                        b
                        """);
    }
}
