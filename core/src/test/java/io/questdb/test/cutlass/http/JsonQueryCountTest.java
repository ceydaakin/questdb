/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.http;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractTest;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

/**
 * With count=true, /exec reports the size of the whole result set, whichever
 * page the limit parameter selects, including pages past the last row.
 */
public class JsonQueryCountTest extends AbstractTest {
    private static final TestHttpClient testHttpClient = new TestHttpClient();

    @AfterClass
    public static void tearDownStatic() {
        testHttpClient.close();
        AbstractTest.tearDownStatic();
        assert Unsafe.getMemUsedByTag(MemoryTag.NATIVE_HTTP_CONN) == 0;
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        testHttpClient.setKeepConnection(false);
    }

    @Test
    public void testCountWhenLimitStartsPastLastRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> getSimpleTester().run((engine, sqlExecutionContext) -> {
            engine.execute("CREATE TABLE t AS (SELECT x FROM long_sequence(10))", sqlExecutionContext);
            engine.execute("CREATE TABLE empty (x LONG)", sqlExecutionContext);
            // t's cursor knows its size, the filtered one does not
            for (String sql : new String[]{"SELECT x FROM t", "SELECT x FROM t WHERE x > 0"}) {
                assertPage(sql, "0,5", "[1],[2],[3],[4],[5]", 10);
                assertPage(sql, "9,20", "[9],[10]", 10);
                assertPage(sql, "10,20", "[10]", 10);
                assertPage(sql, "11,20", "", 10);
                assertPage(sql, "12,20", "", 10);
                assertPage(sql, "20,30", "", 10);
            }
            assertPage("SELECT x FROM t WHERE x > 7", "4,6", "", 3);
            assertPage("SELECT x FROM empty", "0,5", "", 0);
            assertPage("SELECT x FROM empty", "2,5", "", 0);
        }));
    }

    private static void assertPage(String sql, String limit, String rows, long count) {
        CharSequenceObjHashMap<String> params = new CharSequenceObjHashMap<>();
        params.put("limit", limit);
        params.put("count", "true");
        testHttpClient.assertGet(
                "/exec",
                "{\"query\":\"" + sql + "\"," +
                        "\"columns\":[{\"name\":\"x\",\"type\":\"LONG\"}]," +
                        "\"timestamp\":-1," +
                        "\"dataset\":[" + rows + "]," +
                        "\"count\":" + count + "}",
                sql,
                params
        );
    }
}
