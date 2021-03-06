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

package com.ning.metrics.serialization.thrift;

import com.ning.metrics.serialization.thrift.item.DataItem;
import com.ning.metrics.serialization.thrift.item.DataItemDeserializer;
import com.ning.metrics.serialization.thrift.item.DataItemFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class TestThriftFieldImpl
{
    private final DataItemDeserializer dataItemDeserializer = new DataItemDeserializer();

    private final ThriftField nullField = new ThriftField()
    {
        @Override
        public short getId()
        {
            return -1;
        }

        @Override
        public DataItem getDataItem()
        {
            return null;
        }

        @Override
        public void write(final TProtocol protocol)
        {
        }
    };

    @Test(groups = "fast")
    public void testBoolean() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 0), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 1), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 0), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 1), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(0), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(1), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(0L), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(1L), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(-1), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create("true"), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create("1"), (short) 0), TType.BOOL);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create("0"), (short) 0), TType.BOOL);
    }

    @Test(groups = "fast")
    public void testByte() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.BYTE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(false), (short) 0), TType.BYTE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 101), (short) 0), TType.BYTE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 103), (short) 0), TType.BYTE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(103), (short) 0), TType.BYTE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(104L), (short) 0), TType.BYTE);
    }

    @Test(groups = "fast")
    public void testShort() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.I16);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(false), (short) 0), TType.I16);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 101), (short) 0), TType.I16);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 102), (short) 0), TType.I16);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(103), (short) 0), TType.I16);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(104L), (short) 0), TType.I16);

        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 10101), (short) 0), TType.I16);
    }

    @Test(groups = "fast")
    public void testInteger() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.I32);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(false), (short) 0), TType.I32);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 101), (short) 0), TType.I32);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 102), (short) 0), TType.I32);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 10101), (short) 0), TType.I32);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(103), (short) 0), TType.I32);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(104L), (short) 0), TType.I32);

        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(342324), (short) 0), TType.I32);
    }

    @Test(groups = "fast")
    public void testLong() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.I64);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(false), (short) 0), TType.I64);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 101), (short) 0), TType.I64);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 102), (short) 0), TType.I64);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 10101), (short) 0), TType.I64);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(103), (short) 0), TType.I64);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(104L), (short) 0), TType.I64);

        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(11234232411L), (short) 0), TType.I64);
    }

    @Test(groups = "fast")
    public void testDouble() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.DOUBLE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(false), (short) 0), TType.DOUBLE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 101), (short) 0), TType.DOUBLE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 102), (short) 0), TType.DOUBLE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 10101), (short) 0), TType.DOUBLE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(103), (short) 0), TType.DOUBLE);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(104L), (short) 0), TType.DOUBLE);

        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(3.1459), (short) 0), TType.DOUBLE);
    }

    @Test(groups = "fast")
    public void testText() throws Exception
    {
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(true), (short) 0), TType.STRING);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(false), (short) 0), TType.STRING);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((byte) 101), (short) 0), TType.STRING);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 102), (short) 0), TType.STRING);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create((short) 10101), (short) 0), TType.STRING);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(103), (short) 0), TType.STRING);
        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create(104L), (short) 0), TType.STRING);

        testPrimitiveShipment(new ThriftFieldImpl(DataItemFactory.create("the-text-lay    here"), (short) 0), TType.STRING);
    }

    private void testPrimitiveShipment(final ThriftField field, final int type) throws Exception
    {
        final ThriftField result = shipPrimitive(field);

        switch (type) {
            case TType.BOOL:
                Assert.assertEquals(result.getDataItem().getBoolean(), field.getDataItem().getBoolean());
                break;
            case TType.BYTE:
                Assert.assertEquals(result.getDataItem().getByte(), field.getDataItem().getByte());
                break;
            case TType.I16:
                Assert.assertEquals(result.getDataItem().getShort(), field.getDataItem().getShort());
                break;
            case TType.I32:
                Assert.assertEquals(result.getDataItem().getInteger(), field.getDataItem().getInteger());
                break;
            case TType.I64:
                Assert.assertEquals(result.getDataItem().getLong(), field.getDataItem().getLong());
                break;
            case TType.DOUBLE:
                Assert.assertEquals(result.getDataItem().getDouble(), field.getDataItem().getDouble());
                break;
            case TType.STRING:
                Assert.assertEquals(result.getDataItem().getString(), field.getDataItem().getString());
                break;
            default:
                throw new RuntimeException(String.format("unknown thrift type: %d", type));
        }
        Assert.assertEquals(field.equals(nullField), false);
    }

    private ThriftField shipPrimitive(final ThriftField field) throws TException
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final TProtocol output = new TBinaryProtocol(new TIOStreamTransport(out));

        field.write(output);

        output.getTransport().close();

        final TProtocol input = new TBinaryProtocol(new TIOStreamTransport(new ByteArrayInputStream(out.toByteArray())));
        final TField tField = input.readFieldBegin();
        final ThriftField result = new ThriftFieldImpl(dataItemDeserializer.fromThrift(input, tField), tField);

        return result;
    }

    @Test
    public void testToByteArray() throws Exception
    {
        ThriftField field = new ThriftFieldImpl(DataItemFactory.create(true), (short) 0);
        Assert.assertEquals(field.toByteArray()[0], (byte) '1');

        field = new ThriftFieldImpl(DataItemFactory.create(false), (short) 0);
        Assert.assertEquals(field.toByteArray()[0], (byte) '0');
    }
}
