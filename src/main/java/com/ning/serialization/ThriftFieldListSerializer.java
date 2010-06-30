/*
 * Copyright 2010 Ning, Inc.
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

package com.ning.serialization;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;
import java.util.List;

public class ThriftFieldListSerializer
{
    public byte[] createPayload(List<ThriftField> thriftFieldList) throws TException
    {
        ByteArrayOutputStream payloadOutputStream = new ByteArrayOutputStream();
        TProtocol payloadProtocol = new TBinaryProtocol(new TIOStreamTransport(payloadOutputStream));

        serialize(payloadProtocol, thriftFieldList);
        payloadProtocol.getTransport().close();

        return payloadOutputStream.toByteArray();
    }

    public void serialize(TProtocol protocol, List<ThriftField> thriftFieldList) throws TException
    {
        protocol.writeStructBegin(new TStruct("ThriftFieldList"));

        for (ThriftField serializer : thriftFieldList) {
            serializer.write(protocol);
        }

        protocol.writeFieldStop();
        protocol.writeStructEnd();
    }
}