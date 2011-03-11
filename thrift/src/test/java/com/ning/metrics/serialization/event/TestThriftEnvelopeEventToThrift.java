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

package com.ning.metrics.serialization.event;

import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftField;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class TestThriftEnvelopeEventToThrift
{
    @Test
    public void testExtractThrift() throws Exception
    {
        final DateTime eventDateTime = new DateTime();
        final String coreHostname = "hostname";
        final String ip = "10.1.2.3";
        final String type = "myType";

        // Native Thrift
        TLoggingEvent event = new TLoggingEvent();
        event.setCoreHostname(coreHostname); // Field 15
        event.setCoreIp(ip); // Field 14
        event.setCoreType(type); // Field 16
        event.setEventDate(eventDateTime.getMillis()); // Field 1

        // Corresponding ThriftEnvelopeEvent
        ArrayList<ThriftField> fields = new ArrayList<ThriftField>();
        fields.add(ThriftField.createThriftField(coreHostname, (short) 15));
        fields.add(ThriftField.createThriftField(ip, (short) 14));
        fields.add(ThriftField.createThriftField(type, (short) 16));
        fields.add(ThriftField.createThriftField(eventDateTime.getMillis(), (short) 1));
        ThriftEnvelope envelope = new ThriftEnvelope("TLoggingEvent", fields);
        ThriftEnvelopeEvent envelopeEvent = new ThriftEnvelopeEvent(eventDateTime, envelope);

        TLoggingEvent event2 = ThriftEnvelopeEventToThrift.extractThrift(TLoggingEvent.class,  envelopeEvent);
        Assert.assertEquals(event2.getCoreHostname(), event.getCoreHostname());
        Assert.assertEquals(event2.getCoreIp(), event.getCoreIp());
        Assert.assertEquals(event2.getCoreType(), event.getCoreType());
        Assert.assertEquals(event2.getEventDate(), event.getEventDate());
    }
}
