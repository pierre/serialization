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

public class TestThriftToThriftEnvelopeEvent
{
    @Test
    public void testExtractEvent() throws Exception
    {
        final DateTime eventDateTime = new DateTime();
        final String coreHostname = "hostname";
        final String ip = "10.1.2.3";
        final String type = "coic";

        TLoggingEvent event = new TLoggingEvent();
        event.setCoreHostname(coreHostname);
        event.setCoreIp(ip);
        event.setCoreType(type);
        event.setEventDate(eventDateTime.getMillis());

        ThriftEnvelopeEvent envelopeEvent = ThriftToThriftEnvelopeEvent.extractEvent("TLoggingEvent", event);
        TLoggingEvent finalEvent = ThriftEnvelopeEventToThrift.extractThrift(TLoggingEvent.class, envelopeEvent);

        Assert.assertEquals(finalEvent.getCoreHostname(), event.getCoreHostname());
        Assert.assertEquals(finalEvent.getCoreHostname(), coreHostname);

        Assert.assertEquals(finalEvent.getCoreIp(), event.getCoreIp());
        Assert.assertEquals(finalEvent.getCoreIp(), ip);

        Assert.assertEquals(finalEvent.getCoreType(), event.getCoreType());
        Assert.assertEquals(finalEvent.getCoreType(), type);

        Assert.assertEquals(finalEvent.getEventDate(), event.getEventDate());
        Assert.assertEquals(finalEvent.getEventDate(), eventDateTime.getMillis());

        // Make sure fields start at 1, not 0 (Thrift convention)
        for (ThriftField field : ((ThriftEnvelope) envelopeEvent.getData()).getPayload()) {
            Assert.assertTrue(field.getId() > 0);
        }
    }
}
