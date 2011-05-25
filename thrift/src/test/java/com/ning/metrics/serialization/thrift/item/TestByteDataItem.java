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

import com.ning.metrics.serialization.thrift.item.ByteDataItem;
import com.ning.metrics.serialization.thrift.item.DataItem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

public class TestByteDataItem
{
    private final DataItem value0 = new ByteDataItem((byte) 0);
    private final DataItem value1 = new ByteDataItem((byte) 1);
    private final DataItem value127 = new ByteDataItem((byte) 127);
    private final DataItem valueMinus128 = new ByteDataItem((byte) -128);

    @Test(groups = "fast")
    public void testNoArgConstructor() throws Exception
    {
        final DataItem item = new ByteDataItem();
        Assert.assertEquals(item.getByte(), Byte.valueOf((byte) 0));
    }

    @Test(groups = "fast")
    public void testConstructor() throws Exception
    {
        final DataItem item1 = new ByteDataItem((byte) 2);
        Assert.assertEquals(item1.getByte(), Byte.valueOf((byte) 2));
    }

    @Test(groups = "fast")
    public void testConvertToBoolean() throws Exception
    {
        Assert.assertEquals(value0.getBoolean().booleanValue(), false);
        Assert.assertEquals(value1.getBoolean().booleanValue(), true);
        Assert.assertEquals(value127.getBoolean().booleanValue(), true);
        Assert.assertEquals(valueMinus128.getBoolean().booleanValue(), true);
    }

    @Test(groups = "fast")
    public void testConvertToByte() throws Exception
    {
        Assert.assertEquals(value0.getByte(), Byte.valueOf((byte) 0));
        Assert.assertEquals(value1.getByte(), Byte.valueOf((byte) 1));
        Assert.assertEquals(value127.getByte(), Byte.valueOf((byte) 127));
        Assert.assertEquals(valueMinus128.getByte(), Byte.valueOf((byte) -128));
    }

    @Test(groups = "fast")
    public void testConvertToShort() throws Exception
    {
        Assert.assertEquals(value0.getShort(), Short.valueOf((short) 0));
        Assert.assertEquals(value1.getShort(), Short.valueOf((short) 1));
        Assert.assertEquals(value127.getShort(), Short.valueOf((short) 127));
        Assert.assertEquals(valueMinus128.getShort(), Short.valueOf((short) -128));
    }

    @Test(groups = "fast")
    public void testConvertToInteger() throws Exception
    {
        Assert.assertEquals(value0.getInteger(), Integer.valueOf(0));
        Assert.assertEquals(value1.getInteger(), Integer.valueOf(1));
        Assert.assertEquals(value127.getInteger(), Integer.valueOf(127));
        Assert.assertEquals(valueMinus128.getInteger(), Integer.valueOf(-128));
    }

    @Test(groups = "fast")
    public void testConvertToLong() throws Exception
    {
        Assert.assertEquals(value0.getLong(), Long.valueOf(0L));
        Assert.assertEquals(value1.getLong(), Long.valueOf(1L));
        Assert.assertEquals(value127.getLong(), Long.valueOf(127L));
        Assert.assertEquals(valueMinus128.getLong(), Long.valueOf(-128L));
    }

    @Test(groups = "fast")
    public void testConvertToDouble() throws Exception
    {
        Assert.assertEquals(value0.getDouble(), 0.0);
        Assert.assertEquals(value1.getDouble(), 1.0);
        Assert.assertEquals(value127.getDouble(), 127.0);
        Assert.assertEquals(valueMinus128.getDouble(), -128.0);
    }

    @Test(groups = "fast")
    public void testConvertToStringOk() throws Exception
    {
        Assert.assertEquals(value0.getString(), "0");
        Assert.assertEquals(value1.getString(), "1");
        Assert.assertEquals(value127.getString(), "127");
        Assert.assertEquals(valueMinus128.getString(), "-128");
    }

    @Test(groups = "fast")
    public void testCompareToAndEquals() throws Exception
    {
        Assert.assertTrue(value0.compareTo(value1) < 0);
        Assert.assertTrue(value1.compareTo(value127) < 0);
        Assert.assertTrue(value127.compareTo(value127) == 0);
        Assert.assertTrue(value127.compareTo(valueMinus128) > 0);
        Assert.assertEquals(value127, value127);
        Assert.assertEquals(value127.hashCode(), new ByteDataItem((byte) 127).hashCode());
    }

    @Test(groups = "fast")
    public void testToString() throws Exception
    {
        Assert.assertEquals(value0.toString(), value0.getString());
        Assert.assertEquals(value1.toString(), value1.getString());
        Assert.assertEquals(value127.toString(), value127.getString());
        Assert.assertEquals(valueMinus128.toString(), valueMinus128.getString());
    }

    @Test(groups = "fast")
    public void testReadAndWrite() throws Exception
    {
        final ByteDataItem item = new ByteDataItem((byte) 42);
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(byteOut);
        item.write(out);
        final DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
        final DataItem inItem = new ByteDataItem();
        final int type = in.readByte();
        Assert.assertEquals(type, DataItem.BYTE_TYPE);
        inItem.readFields(in);
        Assert.assertEquals(item, inItem);
    }
}
