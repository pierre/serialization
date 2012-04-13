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

import com.ning.metrics.serialization.event.SmileEnvelopeEvent;

import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class TestSmileOutputStream
{
    private SmileFactory f;

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
    public void testFromByteArray() throws Exception
    {
        final String eventType = "hello";
        final SmileOutputStream stream = new SmileOutputStream(eventType, 1024);
        stream.write(createSmilePayload());

        final List<SmileEnvelopeEvent> events = SmileEnvelopeEventDeserializer.extractEvents(new ByteArrayInputStream(stream.toByteArray()));

        Assert.assertEquals(events.size(), 1);
    }

    private byte[] createSmilePayload() throws IOException
    {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();

        SmileEnvelopeEventSerializer serializer = new SmileEnvelopeEventSerializer(false);
        serializer.open(stream);
        serializer.serialize(makeSampleEvent());
        serializer.close();

        return stream.toByteArray();
    }

    private SmileEnvelopeEvent makeSampleEvent() throws IOException
    {
        final HashMap<String, Object> map = new HashMap<String, Object>();

        map.put("firstName", "joe");
        map.put("lastName", "sixPack");
        map.put("theNumberFive", 5);

        return new SmileEnvelopeEvent("sample", new DateTime(), map);
    }
}
