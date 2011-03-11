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

import com.ning.metrics.serialization.thrift.item.BooleanDataItem;
import com.ning.metrics.serialization.thrift.item.DataItem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

public class TestBooleanDataItem
{
    private final DataItem trueDataItem = new BooleanDataItem(true);
    private final DataItem falseDataItem = new BooleanDataItem(false);

    @Test(groups = "fast")
    public void testNoArgConstructor() throws Exception
    {
        DataItem item = new BooleanDataItem();
        Assert.assertEquals(item.getBoolean().booleanValue(), true);
    }

    @Test(groups = "fast")
    public void testConstructor() throws Exception
    {
        DataItem item1 = new BooleanDataItem(true);
        Assert.assertEquals(item1.getBoolean().booleanValue(), true);
        DataItem item2 = new BooleanDataItem(false);
        Assert.assertEquals(item2.getBoolean().booleanValue(), false);
    }

    @Test(groups = "fast")
    public void testConvertToByte() throws Exception
    {
        Assert.assertEquals(trueDataItem.getByte().byteValue(), (byte) 1);
        Assert.assertEquals(falseDataItem.getByte().byteValue(), (byte) 0);
    }

    @Test(groups = "fast")
    public void testConvertToBoolean() throws Exception
    {
        Assert.assertEquals(trueDataItem.getBoolean().booleanValue(), true);
        Assert.assertEquals(falseDataItem.getBoolean().booleanValue(), false);
    }

    @Test(groups = "fast")
    public void testConvertToShort() throws Exception
    {
        Assert.assertEquals(trueDataItem.getShort().shortValue(), (short) 1);
        Assert.assertEquals(falseDataItem.getShort().shortValue(), (short) 0);
    }

    @Test(groups = "fast")
    public void testConvertToInteger() throws Exception
    {
        Assert.assertEquals(trueDataItem.getInteger().intValue(), 1);
        Assert.assertEquals(falseDataItem.getInteger().intValue(), 0);
    }

    @Test(groups = "fast")
    public void testConvertToLong() throws Exception
    {
        Assert.assertEquals(trueDataItem.getLong().longValue(), 1L);
        Assert.assertEquals(falseDataItem.getLong().longValue(), 0L);
    }

    @Test(groups = "fast")
    public void testConvertToDouble() throws Exception
    {
        Assert.assertEquals(trueDataItem.getDouble(), 1.0);
        Assert.assertEquals(falseDataItem.getDouble(), 0.0);
    }

    @Test(groups = "fast")
    public void testConvertToStringOk() throws Exception
    {
        Assert.assertEquals(trueDataItem.getString(), "1");
        Assert.assertEquals(falseDataItem.getString(), "0");
    }

    @Test(groups = "fast")
    public void testCompareToAndEquals() throws Exception
    {
        Assert.assertTrue(trueDataItem.compareTo(new BooleanDataItem(false)) > 0);
        BooleanDataItem trueDataItem2 = new BooleanDataItem(true);
        Assert.assertEquals(trueDataItem, trueDataItem2);
        Assert.assertEquals(trueDataItem.hashCode(), trueDataItem2.hashCode());
    }

    @Test(groups = "fast")
    public void testToString() throws Exception
    {
        Assert.assertEquals(trueDataItem.toString(), trueDataItem.getString());
        Assert.assertEquals(falseDataItem.toString(), falseDataItem.getString());
    }

    @Test(groups = "fast")
    public void testReadAndWrite() throws Exception
    {
        BooleanDataItem item = new BooleanDataItem(true);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOut);
        item.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
        DataItem inItem = new BooleanDataItem();
        int type = in.readByte(); //length must be read outside
        Assert.assertEquals(type, DataItem.BOOLEAN_TYPE);
        inItem.readFields(in);
        Assert.assertEquals(item, inItem);
    }
}
