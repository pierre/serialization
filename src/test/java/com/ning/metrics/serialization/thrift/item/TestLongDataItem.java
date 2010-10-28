package com.ning.metrics.serialization.thrift.item;

import com.ning.metrics.serialization.thrift.item.DataItem;
import com.ning.metrics.serialization.thrift.item.LongDataItem;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

public class TestLongDataItem
{
    private final DataItem value0 = new LongDataItem(0);
    private final DataItem value1 = new LongDataItem(1);
    private final DataItem valueBigPositive = new LongDataItem(10000000000L);
    private final DataItem valueBigNegative = new LongDataItem(-10000000000L);

    @Test(groups = "fast")
    public void testNoArgConstructor() throws Exception
    {
        DataItem item = new LongDataItem();
        Assert.assertEquals(item.getLong(), Long.valueOf(0));
    }

    @Test(groups = "fast")
    public void testConstructor() throws Exception
    {
        DataItem item1 = new LongDataItem(10000000000L);
        Assert.assertEquals(item1.getLong(), Long.valueOf(10000000000L));
    }

    @Test(groups = "fast")
    public void testConvertToBoolean() throws Exception
    {
        Assert.assertEquals(value0.getBoolean().booleanValue(), false);
        Assert.assertEquals(value1.getBoolean().booleanValue(), true);
        Assert.assertEquals(valueBigPositive.getBoolean().booleanValue(), true);
        Assert.assertEquals(valueBigNegative.getBoolean().booleanValue(), true);
    }

    @Test(groups = "fast")
    public void testConvertToByte() throws Exception
    {
        Assert.assertEquals(value0.getByte(), Byte.valueOf((byte) 0));
        Assert.assertEquals(value1.getByte(), Byte.valueOf((byte) 1));
        Assert.assertEquals(valueBigPositive.getByte(), Byte.valueOf((byte) 0)); //overflow
        Assert.assertEquals(valueBigNegative.getByte(), Byte.valueOf((byte) 0)); //overflow
    }

    @Test(groups = "fast")
    public void testConvertToShort() throws Exception
    {
        Assert.assertEquals(value0.getShort(), Short.valueOf((short) 0));
        Assert.assertEquals(value1.getShort(), Short.valueOf((short) 1));
        Assert.assertEquals(valueBigPositive.getShort(), Short.valueOf((short) -7168)); //overflow
        Assert.assertEquals(valueBigNegative.getShort(), Short.valueOf((short) 7168)); //overflow
    }

    @Test(groups = "fast")
    public void testConvertToInteger() throws Exception
    {
        Assert.assertEquals(value0.getInteger(), Integer.valueOf(0));
        Assert.assertEquals(value1.getInteger(), Integer.valueOf(1));
        Assert.assertEquals(valueBigPositive.getInteger(), Integer.valueOf(1410065408)); //overflow
        Assert.assertEquals(valueBigNegative.getInteger(), Integer.valueOf(-1410065408)); //overflow
    }

    @Test(groups = "fast")
    public void testConvertToLong() throws Exception
    {
        Assert.assertEquals(value0.getLong(), Long.valueOf(0));
        Assert.assertEquals(value1.getLong(), Long.valueOf(1));
        Assert.assertEquals(valueBigPositive.getLong(), Long.valueOf(10000000000L));
        Assert.assertEquals(valueBigNegative.getLong(), Long.valueOf(-10000000000L));
    }

    @Test(groups = "fast")
    public void testConvertToDouble() throws Exception
    {
        Assert.assertEquals(value0.getDouble(), 0.0);
        Assert.assertEquals(value1.getDouble(), 1.0);
        Assert.assertEquals(valueBigPositive.getDouble(), 10000000000.0);
        Assert.assertEquals(valueBigNegative.getDouble(), -10000000000.0);
    }

    @Test(groups = "fast")
    public void testConvertToStringOk() throws Exception
    {
        Assert.assertEquals(value0.getString(), "0");
        Assert.assertEquals(value1.getString(), "1");
        Assert.assertEquals(valueBigPositive.getString(), "10000000000");
        Assert.assertEquals(valueBigNegative.getString(), "-10000000000");
    }

    @Test(groups = "fast")
    public void testCompareToAndEquals() throws Exception
    {
        Assert.assertTrue(value0.compareTo(value1) < 0);
        Assert.assertTrue(value1.compareTo(value1) == 0);
        Assert.assertTrue(valueBigPositive.compareTo(valueBigNegative) > 0);
        Assert.assertEquals(valueBigPositive, new LongDataItem(10000000000L));
        Assert.assertEquals(valueBigPositive.hashCode(), new LongDataItem(10000000000L).hashCode());
    }

    @Test(groups = "fast")
    public void testToString() throws Exception
    {
        Assert.assertEquals(value0.toString(), "0");
        Assert.assertEquals(value1.toString(), "1");
        Assert.assertEquals(valueBigPositive.toString(), "10000000000");
        Assert.assertEquals(valueBigNegative.toString(), "-10000000000");
    }

    @Test(groups = "fast")
    public void testReadAndWrite() throws Exception
    {
        DataItem item = new LongDataItem(20000000000L);
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOut);
        item.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
        DataItem inItem = new LongDataItem();
        int type = in.readByte();
        Assert.assertEquals(type, DataItem.LONG_TYPE);
        inItem.readFields(in);
        Assert.assertEquals(item, inItem);
    }
}