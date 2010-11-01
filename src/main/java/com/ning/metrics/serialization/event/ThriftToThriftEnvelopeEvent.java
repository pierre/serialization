/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.serialization.event;

import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftField;
import com.ning.metrics.serialization.thrift.ThriftFieldListDeserializer;
import org.apache.thrift.TException;
import org.joda.time.DateTime;

import java.util.List;

public class ThriftToThriftEnvelopeEvent
{
    /**
     * Given a serialized Thrift, generate a ThrifTEnvelopeEvent
     *
     * @param type    Thrift schema name
     * @param payload serialized Thrift
     * @return ThriftEnvelopeEvent representing the Thrift (the event timestamp defaults to now())
     * @throws TException if the payload is not a valid Thrift
     */
    public static Event extractEvent(String type, byte[] payload) throws TException
    {
        return extractEvent(type, new DateTime(), payload);
    }

    /**
     * Given a serialized Thrift, generate a ThrifTEnvelopeEvent
     *
     * @param type          Thrift schema name
     * @param eventDateTime the event timestamp
     * @param payload       serialized Thrift
     * @return ThriftEnvelopeEvent representing the Thrift
     * @throws TException if the payload is not a valid Thrift
     */
    public static Event extractEvent(String type, DateTime eventDateTime, byte[] payload) throws TException
    {
        final List<ThriftField> list = new ThriftFieldListDeserializer().readPayload(payload);
        final ThriftEnvelope envelope = new ThriftEnvelope(type, list);

        return new ThriftEnvelopeEvent(eventDateTime, envelope);
    }
}
