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

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

public class ThriftEnvelopeSerialization implements Serialization<ThriftEnvelope>
{
    static final short TYPE_ID = 0;
    static final short PAYLOAD_ID = 1;
    static final short NAME_ID = 2;

    @Override
    public boolean accept(Class<?> c)
    {
        return ThriftEnvelope.class.isAssignableFrom(c);
    }

    @Override
    public Deserializer<ThriftEnvelope> getDeserializer(Class<ThriftEnvelope> c)
    {
        return new ThriftEnvelopeDeserializer();
    }

    @Override
    public Serializer<ThriftEnvelope> getSerializer(Class<ThriftEnvelope> c)
    {
        return new ThriftEnvelopeSerializer();
    }
}
