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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

public class ThriftFieldListParser
{
    private final ThriftFieldListDeserializer deserializer = new ThriftFieldListDeserializer();

    public ArrayList<ThriftField> parse(Integer contentLength, InputStream input) throws IOException, IllegalArgumentException
    {
        if (contentLength == 0 || input == null) {
            throw new IllegalArgumentException("unable to parse Thrift field list from binary data");
        }

        byte[] buffer = new byte[contentLength];
        int totalBytesRead = 0;
        int bytesRead = 0;

        while (bytesRead != -1 && totalBytesRead < contentLength) {
            bytesRead = input.read(buffer, totalBytesRead, contentLength - totalBytesRead);
            totalBytesRead += bytesRead;
        }

        try {
            ArrayList<ThriftField> thriftFieldList = new ArrayList<ThriftField>();
            thriftFieldList.addAll(deserializer.readPayload(buffer));

            return thriftFieldList;
        }
        catch (TException e) {
            throw new IllegalArgumentException("unable to parse Thrift field list from binary data", e);
        }
    }
}
