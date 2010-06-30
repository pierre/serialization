package com.ning.hadoop.thrift.serialization;

import com.ning.serialization.BooleanDataItem;
import com.ning.serialization.DataItem;
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