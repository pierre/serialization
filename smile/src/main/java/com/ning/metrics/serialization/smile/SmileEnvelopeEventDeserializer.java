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

import com.ning.metrics.serialization.event.EventDeserializer;
import com.ning.metrics.serialization.event.SmileEnvelopeEvent;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.LinkedList;
import java.util.List;

public class SmileEnvelopeEventDeserializer implements EventDeserializer
{
    protected static final SmileFactory smileFactory = new SmileFactory();
    protected static final JsonFactory jsonFactory = new JsonFactory();

    static {
        // yes, full 'compression' by checking for repeating names, short string values:
        smileFactory.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        smileFactory.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        // and for now let's not mandate header for input
        smileFactory.configure(SmileParser.Feature.REQUIRE_HEADER, false);
    }

    private static final ObjectMapper smileObjectMapper = new ObjectMapper(smileFactory);
    private static final ObjectMapper jsonObjectMapper = new ObjectMapper(jsonFactory);
    private static final byte SMILE_MARKER = ':';

    private final JsonParser parser;
    private final ObjectMapper mapper;

    private boolean hasFailed = false;

    // used by hasNextEvent()
    // keeps track of the first JsonToken that is NOT extracted
    private JsonToken nextToken = null;

    /**
     * SmileEnvelopeEventDeserializer should be instantiated (using this constructor) if and only if
     * you plan on using it to incrementally extract events (rather than extracting them all at once)
     *
     * @param in        InputStream containing events
     * @param plainJson whether the stream is in plain json (otherwise smile)
     * @throws IOException generic I/O exception
     */
    public SmileEnvelopeEventDeserializer(final InputStream in, final boolean plainJson) throws IOException
    {
        // TODO bug when using pushbackInputStream like extractEvents does. very strange.

        if (!plainJson) {
            parser = smileFactory.createJsonParser(in);
            mapper = smileObjectMapper;
        }
        else {
            parser = jsonFactory.createJsonParser(in);
            mapper = jsonObjectMapper;
        }

        /* check that we either point to START_ARRAY or START_OBJECT; in former case
         * it is assumed we have array of event objects; in latter case just a
         * sequence of event objects.
         */
        nextToken = parser.nextToken();
        boolean inArray = (nextToken == JsonToken.START_ARRAY);
        if (inArray) {
            nextToken = parser.nextToken();
        }
        // either way, first 'real' event must be a JSON Object
        if (nextToken != JsonToken.START_OBJECT) {
            throw new IOException("Invalid stream: expected JsonToken.START_OBJECT (in array context? "
                    +inArray+"): instead encountered: "+nextToken);
        }
    }

    public boolean hasFailed() { return hasFailed; }
    
    public boolean hasNextEvent()
    {
        try {
            return _hasNextEvent();
        } catch (Exception e) {
            hasFailed = true;
            return false;
        }
    }

    private boolean _hasNextEvent() throws IOException
    {
        if (hasFailed) {
            return false;
        }

        // don't advance nextToken if you don't have to

        if (nextToken == null) {
            // get next token
            nextToken = parser.nextToken();
            // and if it's the end of array (or input), close explicitly
            if (nextToken == null || nextToken == JsonToken.END_ARRAY) {
                parser.close();
            }
        }
        // could verify that it is JsonToken.START_OBJECT?
        return nextToken != JsonToken.END_ARRAY && nextToken != null;
    }

    /**
     * Extracts the next event in the stream.
     * Note: Stream must be formatted as an array of (serialized) SmileEnvelopeEvents.
     *
     * @return nextEvent. return null if it reaches the end of the list
     * @throws IOException if there's a parsing issue
     */
    public SmileEnvelopeEvent getNextEvent() throws IOException
    {
        try {
            if (!_hasNextEvent()) {
                return null;
            }
            final JsonNode node = mapper.readValue(parser, JsonNode.class);
            nextToken = null; // reset nextToken

            return new SmileEnvelopeEvent(node);
        }
        // make sure we don't return true for hasNextEvent after this
        catch (IOException e) {
            hasFailed = true;
            throw e;
        }
    }

    /**
     * Extracts all events in the stream
     * Note: Stream must be formatted as an array of (serialized) SmileEnvelopeEvents.
     *
     * @param in InputStream containing events
     * @return A list of SmileEnvelopeEvents
     * @throws IOException generic I/O exception
     */
    public static List<SmileEnvelopeEvent> extractEvents(final InputStream in) throws IOException
    {
        final PushbackInputStream pbIn = new PushbackInputStream(in);

        final byte firstByte = (byte) pbIn.read();

        // EOF?
        if (firstByte == -1) {
            return null;
        }

        pbIn.unread(firstByte);

        SmileEnvelopeEventDeserializer deser = (firstByte == SMILE_MARKER) ?
                new SmileEnvelopeEventDeserializer(pbIn, false) :
                new SmileEnvelopeEventDeserializer(pbIn, true);
        final List<SmileEnvelopeEvent> events = new LinkedList<SmileEnvelopeEvent>();

        while (deser.hasNextEvent()) {
            SmileEnvelopeEvent event = deser.getNextEvent();
            if (event == null) {
                // TODO: this is NOT expected, should warn
                break;
            }
            events.add(event);
        }
        return events;
    }
}
