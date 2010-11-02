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
        String value = null;
        try {
            DataItem item = new StringDataItem(value);
            Assert.fail("expected NullPointerException");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getClass(), NullPointerException.class);
        }
    }

    @Test(groups = "fast")
    public void testNoArgConstructor() throws Exception
    {
        DataItem item = new StringDataItem();
        Assert.assertEquals(item.getString(), "");
    }

    @Test(groups = "fast")
    public void testConstructor() throws Exception
    {
        String testString = "test-string";
        DataItem item1 = new StringDataItem(testString);
        Assert.assertEquals(item1.getString(), testString);
    }

    @Test(groups = "fast")
    public void testConvertToDoubleOk() throws Exception
    {
        long value1 = 1341345143;
        DataItem item1 = new StringDataItem(String.valueOf(value1));
        Assert.assertEquals(item1.getDouble(), (double) value1);
        double value2 = 1341345143.13242142;
        DataItem item2 = new StringDataItem(String.valueOf(value2));
        Assert.assertEquals(item2.getDouble(), value2);
    }

    @Test(groups = "fast")
    public void testConvertToDoubleFail() throws Exception
    {
        try {
            DataItem item = new StringDataItem("a string");
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
        long value = 7890;
        DataItem item = new StringDataItem(String.valueOf(value));
        Assert.assertEquals(item.getLong(), Long.valueOf(value));
    }

    @Test(groups = "fast")
    public void testConvertToLongFail() throws Exception
    {
        try {
            DataItem item = new StringDataItem("a string");
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
        String value1 = "value 1";
        String value1a = "value 1";
        String value2 = "value 2";
        DataItem item1 = new StringDataItem(value1);
        DataItem item1a = new StringDataItem(value1a);
        DataItem item2 = new StringDataItem(value2);
        Assert.assertEquals(item1, item1a);
        Assert.assertEquals(item1.hashCode(), item1a.hashCode());
        Assert.assertTrue(item1.compareTo(item2) < 0);
        Assert.assertTrue(item2.compareTo(item1) > 0);
        Assert.assertTrue(item1.hashCode() != item2.hashCode());
    }

    @Test(groups = "fast")
    public void testToString() throws Exception
    {
        DataItem item = new StringDataItem("...a.a.sd.f...sf");
        Assert.assertEquals(item.toString(), item.getString());
    }


    @Test(groups = "fast")
    public void testReadAndWrite() throws Exception
    {
        StringDataItem item = new StringDataItem("some funky text");
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOut);
        item.write(out);
        DataInput in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()));
        StringDataItem inItem = new StringDataItem();
        int type = in.readByte(); //length must be read outside
        Assert.assertEquals(type, DataItem.STRING_TYPE);
        inItem.readFields(in);
        Assert.assertEquals(item, inItem);
    }


}