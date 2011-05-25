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

package com.ning.metrics.serialization.thrift.hadoop;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ThriftWritableDeserializer
{
    private DataInputStream dataIn;

    public void open(final InputStream in)
    {
        if (in instanceof DataInputStream) {
            dataIn = (DataInputStream) in;
        }
        else {
            dataIn = new DataInputStream(in);
        }
    }

    public ThriftWritable deserialize(final ThriftWritable writable) throws IOException
    {
        writable.readFields(dataIn);
        return writable;
    }

    public void close() throws IOException
    {
        dataIn.close();
    }
}
