/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.serialization.thrift.item;

import com.ning.metrics.serialization.thrift.item.DataItem;
import com.ning.metrics.serialization.thrift.item.DoubleDataItem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

public class TestDoubleDataItem
{
    private final DataItem value0 = new DoubleDataItem(0);
    private final DataItem value1 = new DoubleDataItem(1);
    private final DataItem valueWithDecimal = new DoubleDataItem(1.1415);
    private final DataItem valueBigPositive = new DoubleDataItem(1000000000.11111);
    private final DataItem valueBigNegative = new DoubleDataItem(-1000000000.11111);

    @Test(groups = "fast")
    public void testNoArgConstructor() throws Exception
    {
        final DataItem item = new DoubleDataItem();
        Assert.assertEquals(item.getDouble(), 0.0);
    }

    @Test(groups = "fast")
    public void testConstructor() throws Exception
    {
        final DataItem item1 = new DoubleDataItem(2.72);
        Assert.assertEquals(item1.getDouble(), 2.72);
    }

    @Test(groups = "fast")
    public void testConvertToBoolean() throws Exception
    {
        Assert.assertEquals(value0.getBoolean().booleanValue(), false);
        Assert.assertEquals(value1.getBoolean().booleanValue(), true);
        Assert.assertEquals(valueWithDecimal.getBoolean().booleanValue(), true);
        Assert.assertEquals(valueBigPositive.getBoolean().booleanValue(), true);
        Assert.assertEquals(valueBigNegative.getBoolean().booleanValue(), true);
    }

    @Test(groups = "fast")
    public void testConvertToByte() throws Exception
    {
        Assert.assertEquals(value0.getByte(), Byte.valueOf((byte) 0));
        Assert.assertEquals(value1.getByte(), Byte.valueOf((byte) 1));
        Assert.assertEquals(valueWithDecimal.getByte(), Byte.valueOf((byte) 1));
        Assert.assertEquals(valueBigPositive.getByte(), Byte.valueOf((byte) 0)); //overflow
        Assert.assertEquals(valueBigNegative.getByte(), Byte.valueOf((byte) 0)); //overflow
    }

    @Test(groups = "fast")
    public void testConvertToShort() throws Exception
    {
        Assert.assertEquals(value0.getShort(), Short.valueOf((short) 0));
        Assert.assertEquals(value1.getShort(), Short.valueOf((short) 1));
        Assert.assertEquals(valueWithDecimal.getShort(), Short.valueOf((short) 1));
        Assert.assertEquals(valueBigPositive.getShort(), Short.valueOf((short) -13824)); //overflow
        Assert.assertEquals(valueBigNegative.getShort(), Short.valueOf((short) 13824)); //overflow
    }

    @Test(groups = "fast")
    public void testConvertToInteger() throws Exception
    {
        Assert.assertEquals(value0.getInteger(), Integer.valueOf(0));
        Assert.assertEquals(value1.getInteger(), Integer.valueOf(1));
        Assert.assertEquals(valueWithDecimal.getInteger(), Integer.valueOf(1));
        Assert.assertEquals(valueBigPositive.getInteger(), Integer.valueOf(1000000000));
        Assert.assertEquals(valueBigNegative.getInteger(), Integer.valueOf(-1000000000));
    }

    @Test(groups = "fast")
    public void testConvertToLong() throws Exception
    {
        Assert.assertEquals(value0.getLong(), Long.valueOf(0));
        Assert.assertEquals(value1.getLong(), Long.valueOf(1));
        Assert.assertEquals(valueWithDecimal.getLong(), Long.valueOf(1));
        Assert.assertEquals(valueBigPositive.getLong(), Long.valueOf(1000000000));
        Assert.assertEquals(valueBigNegative.getLong(), Long.valueOf(-1000000000));
    }

    @Test(groups = "fast")
    public void testConvertToDouble() throws Exception
    {
        Assert.assertEquals(value0.getDouble(), 0.0);
        Assert.assertEquals(value1.getDouble(), 1.0);
        Assert.assertEquals(valueWithDecimal.getDouble(), 1.1415);
        Assert.assertEquals(valueBigPositive.getDouble(), 1000000000.11111);
        Assert.assertEquals(valueBigNegative.getDouble(), -1000000000.11111);
    }

    @Test(groups = "fast")
    public void testConvertToStringOk() throws Exception
    {
        Assert.assertEquals(value0.getString(), "0");
        Assert.assertEquals(value1.getString(), "1");
        Assert.assertEquals(valueWithDecimal.getString(), "1.1415");
        Assert.assertEquals(valueBigPositive.getString(), "1.00000000011111E9");
        Assert.assertEquals(valueBigNegative.getString(), "-1.00000000011111E9");
    }

    @Test(groups = "fast")
    public void testCompareToAndEquals() throws Exception
    {
        Assert.assertTrue(value0.compareTo(value1) < 0);
        Assert.assertTrue(value1.compareTo(value1) == 0);
        Assert.assertTrue(valueBigPositive.compareTo(valueBigNegative) > 0);
        Assert.assertEquals(valueBigPositive, new DoubleDataItem(1000000000.11111));
        Assert.assertEquals(valueBigPositive.hashCode(), new DoubleDataItem(1000000000.11111).hashCode());
    }

    @Test(groups = "fast")
    public void testToString() throws Exception
    {
        Assert.assertEquals(value0.toString(), "0");
        Assert.assertEquals(value1.toString(), "1");
        Assert.assertEquals(valueWithDecimal.toString(), "1.1415");
        Assert.assertEquals(valueBigPositive.toString(), "1.00000000011111E9");
        Assert.assertEquals(valueBigNegative.toString(), "-1.00000000011111E9");
    }

    @Test(groups = "fast")
    public void testReadAndWrite() throws Exception
    {
        final DataItem item = new DoubleDataItem(987654321.123456);
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(byteOut);
        item.write(out);
        final DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
        final DataItem inItem = new DoubleDataItem();
        final int type = in.readByte();
        Assert.assertEquals(type, DataItem.DOUBLE_TYPE);
        inItem.readFields(in);
        Assert.assertEquals(item, inItem);
    }
}
