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

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;

public class TestSmileEnvelopeEvent
{
    private static final Granularity eventGranularity = Granularity.MONTHLY;
    private static final DateTime eventDateTime = new DateTime();
    private static final String SCHEMA_NAME = "mySmile";

    private byte[] serializedBytes;

    private String serializedString;

    @BeforeTest
    public void setUp() throws IOException
    {
        // Use same configuration as SmileEnvelopeEvent
        final SmileFactory f = new SmileFactory();
        f.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        f.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        f.configure(SmileParser.Feature.REQUIRE_HEADER, false);

        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final JsonGenerator g = f.createJsonGenerator(stream);

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
        // one sanity check; should be able to round-trip via String (iff using latin-1!)
        serializedString = stream.toString(SmileEnvelopeEvent.CHARSET.toString());
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Unit tests, char-to-byte conversions
    ///////////////////////////////////////////////////////////////////////
     */

    /**
     * Unit test that verifies that messy conversions between byte[] and String do not totally
     * break contents
     *
     * @throws Exception generic serialization exception
     */
    @Test(groups = "fast")
    public void testBytesVsString() throws Exception
    {
        final byte[] fromString = serializedString.getBytes(SmileEnvelopeEvent.CHARSET);
        Assert.assertEquals(fromString, serializedBytes);
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // Unit tests, metadata access
   ///////////////////////////////////////////////////////////////////////
    */

    @Test(groups = "fast")
    public void testGetEventDateTime() throws Exception
    {
        final SmileEnvelopeEvent event = createEvent();
        Assert.assertEquals(event.getEventDateTime(), eventDateTime);
    }

    @Test(groups = "fast")
    public void testGetName() throws Exception
    {
        final SmileEnvelopeEvent event = createEvent();
        Assert.assertEquals(event.getName(), SCHEMA_NAME);
    }

    /*
    ///////////////////////////////////////////////////////////////////////
    // Unit tests, externalization (readObject/writeObject)
    ///////////////////////////////////////////////////////////////////////
     */

    @Test(groups = "fast")
    public void testExternalization() throws Exception
    {
        final SmileEnvelopeEvent envelope = createEvent();

        final byte[] inputBytes = serializedBytes;
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bytes);
        out.writeObject(envelope);
        out.close();
        final byte[] data = bytes.toByteArray();
        final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data));
        final Object ob = in.readObject();

        Assert.assertNotNull(ob);
        Assert.assertSame(SmileEnvelopeEvent.class, ob.getClass());

        final Event result = (SmileEnvelopeEvent) ob;
        // name is not automatically set, but can check other metadata
        Assert.assertSame(result.getGranularity(), eventGranularity);
        Assert.assertEquals(result.getEventDateTime(), eventDateTime);

        Assert.assertNotNull(result.getData());
        // TODO Looks like there are multiple JsonNode classes?
        //Assert.assertSame(result.getData().getClass(), JsonNode.class);
        Assert.assertEquals(result.getSerializedEvent(), inputBytes);
    }

    @SuppressWarnings("deprecation")
    @Test(groups = "fast")
    public void testReadWriteExternal() throws Exception
    {
        final SmileEnvelopeEvent event = createEvent();

        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutputStream(outStream);
        event.writeExternal(out);
        out.close();

        final SmileEnvelopeEvent event2 = new SmileEnvelopeEvent();
        event2.readExternal(new ObjectInputStream(new ByteArrayInputStream(outStream.toByteArray())));

        Assert.assertEquals(event2.getName(), event.getName());
        Assert.assertEquals(event2.getGranularity(), event.getGranularity());
        Assert.assertEquals(event2.getName(), event.getName());
    }

    @Test(groups = "fast")
    public void testStaticUtils() throws Exception
    {
        final SmileEnvelopeEvent event = createEvent();
        Assert.assertEquals(SmileEnvelopeEvent.getEventDateTimeFromJson((JsonNode) event.getData()), eventDateTime);
        Assert.assertEquals(SmileEnvelopeEvent.getGranularityFromJson((JsonNode) event.getData()), eventGranularity);
    }

    @Test(groups = "fast")
    public void testConstructorFromMap() throws Exception
    {
        final HashMap<String, Object> eventMap = new HashMap<String, Object>();
        eventMap.put("foo", "bar");
        eventMap.put("bleh", 12);
        final Event event = new SmileEnvelopeEvent("myEvent", eventDateTime, eventMap);

        Assert.assertEquals(event.getName(), "myEvent");
        Assert.assertEquals(SmileEnvelopeEvent.getEventDateTimeFromJson((JsonNode) event.getData()), eventDateTime);
        Assert.assertEquals(SmileEnvelopeEvent.getGranularityFromJson((JsonNode) event.getData()), Granularity.HOURLY);
        Assert.assertEquals(((JsonNode) event.getData()).get("foo").getValueAsText(), "bar");
        Assert.assertEquals(((JsonNode) event.getData()).get("bleh").getValueAsInt(), 12);
    }

    /*
   ///////////////////////////////////////////////////////////////////////
   // Helper methods
   ///////////////////////////////////////////////////////////////////////
    */

    private SmileEnvelopeEvent createEvent() throws IOException
    {
        return new SmileEnvelopeEvent(SCHEMA_NAME, serializedBytes, eventDateTime, eventGranularity);
    }
}
