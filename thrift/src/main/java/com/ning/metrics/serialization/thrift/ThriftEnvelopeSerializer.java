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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ThriftEnvelopeSerializer
{
    private static final TField TYPE_NAME_FIELD = new TField("typeName", TType.STRING, ThriftEnvelopeSerialization.TYPE_ID);
    private static final TField PAYLOAD_FIELD = new TField("payload", TType.STRING, ThriftEnvelopeSerialization.PAYLOAD_ID);
    private static final TField NAME_FIELD = new TField("name", TType.STRING, ThriftEnvelopeSerialization.NAME_ID);

    private TProtocol protocol;
    private final ThriftFieldListSerializer payloadSerializer = new ThriftFieldListSerializer();

    public void open(final OutputStream out) throws IOException
    {
        protocol = new TBinaryProtocol(new TIOStreamTransport(out));
    }

    public void serialize(final ThriftEnvelope thriftEnvelope) throws IOException
    {
        try {
            protocol.writeStructBegin(new TStruct("EventEnvelope"));

            protocol.writeFieldBegin(TYPE_NAME_FIELD);
            protocol.writeString(thriftEnvelope.getTypeName());
            protocol.writeFieldEnd();

            if (!thriftEnvelope.getTypeName().equals(thriftEnvelope.getName())) {
                protocol.writeFieldBegin(NAME_FIELD);
                protocol.writeString(thriftEnvelope.getName());
                protocol.writeFieldEnd();
            }


            final byte[] payload = payloadSerializer.createPayload(thriftEnvelope.getPayload());

            protocol.writeFieldBegin(PAYLOAD_FIELD);
            protocol.writeBinary(ByteBuffer.wrap(payload));
            protocol.writeFieldEnd();

            protocol.writeFieldStop();
            protocol.writeStructEnd();
        }
        catch (TException e) {
            throw new IOException("error serializing Thrift" + thriftEnvelope, e);
        }
    }

    public void close() throws IOException
    {
        protocol.getTransport().close();
    }
}
