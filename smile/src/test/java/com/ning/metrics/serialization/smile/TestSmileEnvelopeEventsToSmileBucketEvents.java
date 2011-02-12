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

package com.ning.metrics.serialization.smile;

import com.ning.metrics.serialization.event.Granularity;
import com.ning.metrics.serialization.event.SmileBucketEvent;
import com.ning.metrics.serialization.event.SmileEnvelopeEvent;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class TestSmileEnvelopeEventsToSmileBucketEvents
{
    private static Granularity eventGranularity = Granularity.MONTHLY;
    private static final DateTime eventDateTime = new DateTime();

    private byte[] serializedBytes;

    @BeforeTest
    public void setUp() throws IOException
    {
        // Use same configuration as SmileEnvelopeEvent
        SmileFactory f = new SmileFactory();
        f.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        f.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        f.configure(SmileParser.Feature.REQUIRE_HEADER, false);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        JsonGenerator g = f.createJsonGenerator(stream);

        g.writeStartObject();
        g.writeStringField(SmileEnvelopeEvent.SMILE_EVENT_GRANULARITY_TOKEN_NAME, eventGranularity.toString());
        g.writeObjectFieldStart("name");
        g.writeStringField("first", "Joe");
        g.writeStringField("last", "Sixpack");
        g.writeEndObject(); // for field 'name'
        g.writeStringField("gender", "MALE");
        g.writeNumberField(SmileEnvelopeEvent.SMILE_EVENT_DATETIME_TOKEN_NAME, eventDateTime.getMillis());
        g.writeBooleanField("verified", false);
        g.writeEndObject();
        g.close(); // important: will force flushing of output, close underlying output stream

        serializedBytes = stream.toByteArray();
    }

    @Test
    public void testExtractEvents() throws Exception
    {
        SmileEnvelopeEvent event1 = createEvent("event1");
        SmileEnvelopeEvent event2 = createEvent("event2");

        ArrayList<SmileEnvelopeEvent> envelopes = new ArrayList<SmileEnvelopeEvent>();
        envelopes.add(event1);
        envelopes.add(event2);

        Collection<SmileBucketEvent> events = SmileEnvelopeEventsToSmileBucketEvents.extractEvents(envelopes);

        Assert.assertEquals(events.size(), 2);
        Assert.assertEquals(((SmileBucketEvent) events.toArray()[0]).getName(), event1.getName());
        Assert.assertEquals(((SmileBucketEvent) events.toArray()[1]).getName(), event2.getName());

        Assert.assertNotSame(((SmileBucketEvent) events.toArray()[0]).getSerializedEvent(), event1.getSerializedEvent());
        Assert.assertNotSame(((SmileBucketEvent) events.toArray()[1]).getSerializedEvent(), event1.getSerializedEvent());
    }

    private SmileEnvelopeEvent createEvent(String schema) throws IOException
    {
        return new SmileEnvelopeEvent(schema, serializedBytes, eventDateTime, eventGranularity);
    }
}
