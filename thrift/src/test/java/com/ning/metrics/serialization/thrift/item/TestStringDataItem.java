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
import com.ning.metrics.serialization.thrift.item.StringDataItem;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

public class TestStringDataItem
{
    @BeforeMethod
    void setup()
    {
    }

    @Test(groups = "fast")
    public void testNoNullAllowed() throws Exception
    {
        try {
            String value = null;
            final DataItem item = new StringDataItem(value);
            Assert.fail("expected NullPointerException");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getClass(), NullPointerException.class);
        }
    }

    @Test(groups = "fast")
    public void testNoArgConstructor() throws Exception
    {
        final DataItem item = new StringDataItem();
        Assert.assertEquals(item.getString(), "");
    }

    @Test(groups = "fast")
    public void testConstructor() throws Exception
    {
        final String testString = "test-string";
        final DataItem item1 = new StringDataItem(testString);
        Assert.assertEquals(item1.getString(), testString);
    }

    @Test(groups = "fast")
    public void testConvertToDoubleOk() throws Exception
    {
        final long value1 = 1341345143;
        final DataItem item1 = new StringDataItem(String.valueOf(value1));
        Assert.assertEquals(item1.getDouble(), (double) value1);
        final double value2 = 1341345143.13242142;
        final DataItem item2 = new StringDataItem(String.valueOf(value2));
        Assert.assertEquals(item2.getDouble(), value2);
    }

    @Test(groups = "fast")
    public void testConvertToDoubleFail() throws Exception
    {
        try {
            final DataItem item = new StringDataItem("a string");
            item.getDouble();
            Assert.fail("expected NumberFormatException NOT thrown");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getClass(), NumberFormatException.class);
        }
    }

    @Test(groups = "fast")
    public void testConvertToLongOk() throws Exception
    {
        final long value = 7890;
        final DataItem item = new StringDataItem(String.valueOf(value));
        Assert.assertEquals(item.getLong(), Long.valueOf(value));
    }

    @Test(groups = "fast")
    public void testConvertToLongFail() throws Exception
    {
        try {
            final DataItem item = new StringDataItem("a string");
            item.getLong();
            Assert.fail("expected NumberFormatException NOT thrown");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getClass(), NumberFormatException.class);
        }
    }

    @Test(groups = "fast")
    public void testCompareToAndEquals() throws Exception
    {
        final String value1 = "value 1";
        final String value1a = "value 1";
        final String value2 = "value 2";
        final DataItem item1 = new StringDataItem(value1);
        final DataItem item1a = new StringDataItem(value1a);
        final DataItem item2 = new StringDataItem(value2);
        Assert.assertEquals(item1, item1a);
        Assert.assertEquals(item1.hashCode(), item1a.hashCode());
        Assert.assertTrue(item1.compareTo(item2) < 0);
        Assert.assertTrue(item2.compareTo(item1) > 0);
        Assert.assertTrue(item1.hashCode() != item2.hashCode());
    }

    @Test(groups = "fast")
    public void testToString() throws Exception
    {
        final DataItem item = new StringDataItem("...a.a.sd.f...sf");
        Assert.assertEquals(item.toString(), item.getString());
    }


    @Test(groups = "fast")
    public void testReadAndWrite() throws Exception
    {
        final StringDataItem item = new StringDataItem("some funky text");
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(byteOut);
        item.write(out);
        final DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
        final StringDataItem inItem = new StringDataItem();
        final int type = in.readByte(); //length must be read outside
        Assert.assertEquals(type, DataItem.STRING_TYPE);
        inItem.readFields(in);
        Assert.assertEquals(item, inItem);
    }


}
