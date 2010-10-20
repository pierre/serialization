package com.ning.hadoop.thrift.serialization;

import com.ning.serialization.BooleanDataItem;
import com.ning.serialization.ByteDataItem;
import com.ning.serialization.DoubleDataItem;
import com.ning.serialization.IntegerDataItem;
import com.ning.serialization.LongDataItem;
import com.ning.serialization.ShortDataItem;
import com.ning.serialization.StringDataItem;
import com.ning.serialization.ThriftEnvelope;
import com.ning.serialization.ThriftEnvelopeDeserializer;
import com.ning.serialization.ThriftEnvelopeSerialization;
import com.ning.serialization.ThriftEnvelopeSerializer;
import com.ning.serialization.ThriftFieldImpl;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class TestThriftEnvelopeSerialization
{
    private final ThriftEnvelopeSerialization serialization = new ThriftEnvelopeSerialization();
    private final ThriftEnvelopeSerializer serializer = new ThriftEnvelopeSerializer();
    private final ThriftEnvelopeDeserializer deserializer = new ThriftEnvelopeDeserializer();

    @Test(groups = "fast")
    public void testSerializeNull() throws Exception
    {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

            serializer.open(byteStream);
            serializer.serialize(null);
            Assert.fail("expected NullPointerException");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getClass(), NullPointerException.class);
        }
    }

    @Test(groups = "fast")
    public void testEmptyObject() throws Exception
    {
        ThriftEnvelope input = new ThriftEnvelope("event-type");
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        serializer.open(byteStream);
        serializer.serialize(input);
        serializer.close();

        deserializer.open(new ByteArrayInputStream(byteStream.toByteArray()));
        ThriftEnvelope result = deserializer.deserialize(null);

        Assert.assertEquals(result, input);
        Assert.assertFalse(result.equals(new ThriftEnvelope("null-struct")));
        deserializer.close();
    }

    @Test(groups = "fast")
    public void testMultipleObjectStream() throws Exception
    {
        ThriftEnvelope input1 = createDummyEvent("input1");
        ThriftEnvelope input2 = createDummyEvent("input2");

        Assert.assertEquals(input1.equals(input2), false);

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        serializer.open(byteStream);
        serializer.serialize(input1);
        serializer.serialize(input2);
        serializer.close();

        deserializer.open(new ByteArrayInputStream(byteStream.toByteArray()));

        ThriftEnvelope result1 = deserializer.deserialize(null);
        Assert.assertEquals(result1, input1);

        ThriftEnvelope result2 = deserializer.deserialize(null);
        Assert.assertEquals(result2, input2);

        deserializer.close();
    }

    @Test(groups = "fast")
    public void testNonNullArgToDeserialize() throws Exception
    {
        ThriftEnvelope input1 = createDummyEvent("fuu-1");
        ThriftEnvelope input2 = createDummyEvent("fuu-2");

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        serializer.open(byteStream);
        serializer.serialize(input1);
        serializer.close();

        deserializer.open(new ByteArrayInputStream(byteStream.toByteArray()));
        ThriftEnvelope result = deserializer.deserialize(input2);
        deserializer.close();

        Assert.assertEquals(input2.getPayload().get(0).getDataItem().getString(), "fuu-1");
        Assert.assertEquals(result, input1);
    }

    @Test(groups = "fast")
    public void testSimpleSerialize() throws Exception
    {
        ThriftEnvelope input = createDummyEvent("fuu");

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        serializer.open(byteStream);
        serializer.serialize(input);
        serializer.close();

        deserializer.open(new ByteArrayInputStream(byteStream.toByteArray()));
        ThriftEnvelope result = deserializer.deserialize(null);
        deserializer.close();

        Assert.assertEquals(result, input);
    }

    @Test(groups = "fast")
    public void testReadOnClosedStream() throws Exception
    {
        try {
            deserializer.open(new ByteArrayInputStream(new byte[0]));
            deserializer.close();
            ThriftEnvelope result = deserializer.deserialize(null);
            Assert.fail("expected IOException");
        }
        catch (Exception e) {
            Assert.assertEquals(e.getClass(), IOException.class);
        }
    }

    @Test(groups = "fast")
    public void testSchemaNameNotSameAsName() throws Exception
    {
        ThriftEnvelope input = createDummyEvent("fuu", "event-name");

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        serializer.open(byteStream);
        serializer.serialize(input);
        serializer.close();

        deserializer.open(new ByteArrayInputStream(byteStream.toByteArray()));
        ThriftEnvelope result = deserializer.deserialize(null);
        deserializer.close();

        Assert.assertEquals(result, input);
    }

    private ThriftEnvelope createDummyEvent(String firstValue)
    {
        return createDummyEvent(firstValue, "event-type");
    }

    private ThriftEnvelope createDummyEvent(String firstValue, String name)
    {
        ThriftEnvelope input = new ThriftEnvelope("event-type", name);

        input.getPayload().add(new ThriftFieldImpl(new StringDataItem(firstValue), new TField("0", TType.STRING, (short) 0)));
        input.getPayload().add(new ThriftFieldImpl(new ByteDataItem((byte) 1), new TField("1", TType.BYTE, (short) 1)));
        input.getPayload().add(new ThriftFieldImpl(new ShortDataItem((short) 2), new TField("2", TType.I16, (short) 2)));
        input.getPayload().add(new ThriftFieldImpl(new IntegerDataItem(4), new TField("3", TType.I32, (short) 3)));
        input.getPayload().add(new ThriftFieldImpl(new LongDataItem(8), new TField("4", TType.I64, (short) 4)));
        input.getPayload().add(new ThriftFieldImpl(new DoubleDataItem(202.1), new TField("5", TType.DOUBLE, (short) 5)));
        input.getPayload().add(new ThriftFieldImpl(new BooleanDataItem(true), new TField("6", TType.BOOL, (short) 6)));

        return input;
    }
}