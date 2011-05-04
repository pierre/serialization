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
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ThriftEnvelopeDeserializer
{
    private TProtocol protocol;
    private final ThriftFieldListDeserializer payloadDeserializer = new ThriftFieldListDeserializer();

    public void open(InputStream in) throws IOException
    {
        protocol = new TBinaryProtocol(new TIOStreamTransport(in));
    }

    public ThriftEnvelope deserialize(ThriftEnvelope thriftEnvelope) throws IOException
    {
        String typeName = null;
        String name = null;
        List<ThriftField> thriftFieldList = new ArrayList<ThriftField>();

        try {
            protocol.readStructBegin();

            TField currentField = protocol.readFieldBegin();
            while (currentField.type != TType.STOP) {
                if (currentField.id == ThriftEnvelopeSerialization.TYPE_ID) {
                    typeName = protocol.readString();
                }
                else if (currentField.id == ThriftEnvelopeSerialization.PAYLOAD_ID) {
                    thriftFieldList.addAll(payloadDeserializer.readPayload(protocol.readBinary().array()));
                }
                else if (currentField.id == ThriftEnvelopeSerialization.NAME_ID) {
                    name = protocol.readString();
                }
                else {
                    throw new IOException(String.format("deserialization error: unknown id: %s", currentField.id));
                }
                protocol.readFieldEnd();
                currentField = protocol.readFieldBegin();
            }

            protocol.readStructEnd();
        }
        catch (TException e) {
            throw new IOException(e);
        }

        if (typeName == null) {
            throw new IOException(String.format("missing type name field, id %d", ThriftEnvelopeSerialization.TYPE_ID));
        }

        if (name == null) {
            name = typeName;
        }

        ThriftEnvelope nextThriftEnvelope = new ThriftEnvelope(typeName, name, thriftFieldList);

        if (thriftEnvelope != null) {
            thriftEnvelope.replaceWith(nextThriftEnvelope);
        }

        return nextThriftEnvelope;
    }

    public void close() throws IOException
    {
        protocol.getTransport().close();
    }
}
