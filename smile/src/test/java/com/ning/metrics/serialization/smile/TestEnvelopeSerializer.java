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

import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.SmileEnvelopeEvent;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class TestEnvelopeSerializer
{
    private Event event;

    @BeforeMethod
    public void setUp() throws Exception
    {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("hello", "world");
        event = new SmileEnvelopeEvent("FuuEvent", new DateTime(), map);
    }

    @Test(groups = "fast")
    public void testSerializerSmile() throws Exception
    {
        testSerializer(false);
    }

    @Test(groups = "fast")
    public void testSerializerJson() throws Exception
    {
        testSerializer(true);

    }

    private void testSerializer(final boolean isPlainJson) throws Exception
    {
        final SmileEnvelopeEventSerializer serializer = new SmileEnvelopeEventSerializer(isPlainJson);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        serializer.open(out);
        final int eventsToWrite = 2;
        for (int i = 0; i < eventsToWrite; i++) {
            serializer.serialize(event);
        }
        serializer.close();

        final InputStream in = new ByteArrayInputStream(out.toByteArray());
        final SmileEnvelopeEventDeserializer deserializer = new SmileEnvelopeEventDeserializer(in, isPlainJson);
        int eventsSeen = 0;
        while (deserializer.hasNextEvent()) {
            eventsSeen++;
            final Event seenEvent = deserializer.getNextEvent();
            Assert.assertEquals(seenEvent, event);
        }

        Assert.assertEquals(eventsSeen, eventsToWrite);
    }
}
