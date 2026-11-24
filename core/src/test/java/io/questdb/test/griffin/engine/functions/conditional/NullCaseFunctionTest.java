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

package io.questdb.test.griffin.engine.functions.conditional;

import io.questdb.griffin.engine.functions.conditional.NullCaseFunction;
import io.questdb.griffin.engine.functions.constants.NullConstant;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Numbers;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every branch of an all-NULL CASE is NULL, so the function reads as NULL in every type, exactly
 * like the NULL constant. A row filter such as {@code x = CASE WHEN y > 0 THEN NULL END} reads it.
 */
public class NullCaseFunctionTest {
    private static final NullCaseFunction function = new NullCaseFunction(null);
    private static final NullConstant reference = NullConstant.NULL;

    @Test
    public void testDecimalsMatchNullConstant() {
        Decimal128 actual128 = new Decimal128();
        Decimal128 expected128 = new Decimal128();
        function.getDecimal128(null, actual128);
        reference.getDecimal128(null, expected128);
        Assert.assertEquals(expected128, actual128);

        Decimal256 actual256 = new Decimal256();
        Decimal256 expected256 = new Decimal256();
        function.getDecimal256(null, actual256);
        reference.getDecimal256(null, expected256);
        Assert.assertEquals(expected256, actual256);

        Assert.assertEquals(reference.getDecimal8(null), function.getDecimal8(null));
        Assert.assertEquals(reference.getDecimal16(null), function.getDecimal16(null));
        Assert.assertEquals(reference.getDecimal32(null), function.getDecimal32(null));
        Assert.assertEquals(reference.getDecimal64(null), function.getDecimal64(null));
    }

    @Test
    public void testFixedSizeGettersReturnNullSentinels() {
        Assert.assertEquals(Numbers.INT_NULL, function.getInt(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getLong(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getDate(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getTimestamp(null));
        Assert.assertTrue(Double.isNaN(function.getDouble(null)));
        Assert.assertTrue(Float.isNaN(function.getFloat(null)));
        Assert.assertEquals(Numbers.IPv4_NULL, function.getIPv4(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getLong128Hi(null));
        Assert.assertEquals(Numbers.LONG_NULL, function.getLong128Lo(null));

        Assert.assertEquals(reference.getBool(null), function.getBool(null));
        Assert.assertEquals(reference.getByte(null), function.getByte(null));
        Assert.assertEquals(reference.getChar(null), function.getChar(null));
        Assert.assertEquals(reference.getShort(null), function.getShort(null));
        Assert.assertEquals(reference.getGeoByte(null), function.getGeoByte(null));
        Assert.assertEquals(reference.getGeoShort(null), function.getGeoShort(null));
        Assert.assertEquals(reference.getGeoInt(null), function.getGeoInt(null));
        Assert.assertEquals(reference.getGeoLong(null), function.getGeoLong(null));
        Assert.assertEquals(reference.getInterval(null), function.getInterval(null));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetRecordCursorFactory() {
        function.getRecordCursorFactory();
    }

    @Test
    public void testVarSizeGettersReturnNull() {
        Assert.assertNull(function.getStrA(null));
        Assert.assertNull(function.getStrB(null));
        Assert.assertEquals(-1, function.getStrLen(null));
        Assert.assertNull(function.getSymbol(null));
        Assert.assertNull(function.getSymbolB(null));
        Assert.assertNull(function.getVarcharA(null));
        Assert.assertNull(function.getVarcharB(null));
        Assert.assertEquals(-1, function.getVarcharSize(null));
        Assert.assertNull(function.getBin(null));
        Assert.assertEquals(-1, function.getBinLen(null));
        Assert.assertSame(reference.getArray(null), function.getArray(null));
        Assert.assertSame(reference.getLong256A(null), function.getLong256A(null));
        Assert.assertSame(reference.getLong256B(null), function.getLong256B(null));

        StringSink sink = new StringSink();
        function.getLong256(null, sink);
        Assert.assertEquals(0, sink.length());
    }
}
