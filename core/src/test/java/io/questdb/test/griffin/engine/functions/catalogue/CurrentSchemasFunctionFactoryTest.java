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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class CurrentSchemasFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCurrentSchemasFunc() throws Exception {
        assertQuery("select x from x where current_schemas(true)[1] = 'public'")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        x
                        1
                        """);
    }

    @Test
    public void testCurrentSchemasFuncInSelect() throws Exception {
        assertQuery("select current_schemas(true) s from long_sequence(1)")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        s
                        {public}
                        """);
    }

    @Test
    public void testCurrentSchemasFuncNextToWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, x INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO base VALUES
                    ('2024-01-01T00:00:00.000000Z', 'a', 1),
                    ('2024-01-01T00:00:01.000000Z', 'b', 2),
                    ('2024-01-01T00:00:02.000000Z', 'a', 3)
                    """);
            assertQuery("SELECT ts, sym, current_schemas(true) v, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) s FROM base")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsym\tv\ts
                            2024-01-01T00:00:00.000000Z\ta\t{public}\t1.0
                            2024-01-01T00:00:01.000000Z\tb\t{public}\t2.0
                            2024-01-01T00:00:02.000000Z\ta\t{public}\t4.0
                            """);
            assertQuery("SELECT ts, sym, sum(x) OVER (PARTITION BY sym ORDER BY ts ROWS 10 PRECEDING) s, current_schemas(true) v FROM base")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tsym\ts\tv
                            2024-01-01T00:00:00.000000Z\ta\t1.0\t{public}
                            2024-01-01T00:00:01.000000Z\tb\t2.0\t{public}
                            2024-01-01T00:00:02.000000Z\ta\t4.0\t{public}
                            """);
            // cached window over a text[] column taken from a sub-query
            assertQuery("SELECT ts, v, rank() OVER (ORDER BY x DESC) r FROM (SELECT ts, x, current_schemas(true) v FROM base)")
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tv\tr
                            2024-01-01T00:00:00.000000Z\t{public}\t3
                            2024-01-01T00:00:01.000000Z\t{public}\t2
                            2024-01-01T00:00:02.000000Z\t{public}\t1
                            """);
        });
    }

    @Test
    public void testPrefixedCurrentSchemasFunc() throws Exception {
        assertQuery("select x from x where pg_catalog.current_schemas(true)[1] = 'public'")
                .ddl("create table x as (select x from long_sequence(1))")
                .expectSize()
                .returns("""
                        x
                        1
                        """);
    }
}
