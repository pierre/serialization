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
    private SmileFactory f;
    private static Granularity eventGranularity = Granularity.MINUTE;
    private static final String EVENT1_NAME = "event1";
    private static final String EVENT2_NAME = "event2";

    @BeforeTest
    public void setUp() throws IOException
    {
        // Use same configuration as SmileEnvelopeEvent
        f = new SmileFactory();
        f.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        f.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        f.configure(SmileParser.Feature.REQUIRE_HEADER, false);
    }

    @Test(groups = "fast")
    public void testExtractEvents() throws Exception
    {
        DateTime eventDateTime = new DateTime(2010, 10, 12, 4, 10, 0, 0);
        // Granularity is by minutes, this forces a new output dir
        DateTime eventDateTime2 = eventDateTime.plusMinutes(3);
        SmileEnvelopeEvent event1 = createEvent(EVENT1_NAME, eventDateTime);
        SmileEnvelopeEvent event2 = createEvent(EVENT2_NAME, eventDateTime2);
        SmileEnvelopeEvent event3 = createEvent(EVENT1_NAME, eventDateTime2);

        ArrayList<SmileEnvelopeEvent> envelopes = new ArrayList<SmileEnvelopeEvent>();
        for (int i = 0; i < 10; i++) {
            envelopes.add(event1);
        }
        for (int i = 0; i < 5; i++) {
            envelopes.add(event2);
        }
        envelopes.add(event3);

        Collection<SmileBucketEvent> events = SmileEnvelopeEventsToSmileBucketEvents.extractEvents(envelopes);

        // We should have 3 buckets:
        //  * event1 at eventDateTime
        //  * event1 at eventDateTime2
        //  * event2 at eventDateTime2
        Assert.assertEquals(events.size(), 3);

        boolean firstBucketSeen = false;
        for (SmileBucketEvent event : events) {
            // We don't really know the order
            if (event.getName().equals(EVENT1_NAME)) {
                if (event.getNumberOfEvent() == 10) {
                    firstBucketSeen = true;
                    Assert.assertEquals(event.getOutputDir("/hello/world"), String.format("/hello/world/%s/2010/10/12/04/10", EVENT1_NAME));
                }
                else {
                    Assert.assertEquals(event.getNumberOfEvent(), 1);
                    Assert.assertEquals(event.getOutputDir("/hello/world"), String.format("/hello/world/%s/2010/10/12/04/13", EVENT1_NAME));
                }
            }
            else {
                Assert.assertEquals(event.getName(), EVENT2_NAME);
                Assert.assertEquals(event.getNumberOfEvent(), 5);
                Assert.assertEquals(event.getOutputDir("/hello/world"), String.format("/hello/world/%s/2010/10/12/04/13", EVENT2_NAME));
            }
        }

        Assert.assertTrue(firstBucketSeen);
    }

    private SmileEnvelopeEvent createEvent(String schema, DateTime eventDateTime) throws IOException
    {
        return new SmileEnvelopeEvent(schema, createSmilePayload(eventDateTime), eventDateTime, eventGranularity);
    }

    private byte[] createSmilePayload(DateTime eventDateTime) throws IOException
    {
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

        return stream.toByteArray();
    }
}
