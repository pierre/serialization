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
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.LinkedList;
import java.util.List;

public class SmileEnvelopeEventDeserializer implements EventDeserializer
{
    private static final Logger log = Logger.getLogger(SmileEnvelopeEventDeserializer.class);
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

        // check that
        if (parser.nextToken() != JsonToken.START_ARRAY) {
            throw new IOException("I can't find a START_ARRAY. The inputStream is supposed to be a list!");
        }
    }

    public boolean hasNextEvent()
    {
        // don't advance nextToken if you don't have to
        if (nextToken != null && nextToken != JsonToken.END_ARRAY) {
            return true;
        }

        try {
            // get next token
            nextToken = parser.nextToken();
            return nextToken != JsonToken.END_ARRAY && nextToken != null;
        }
        catch (Exception e) {
            log.debug("got exception while looking for nextToken");
            return false;
        }
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
        if (!hasNextEvent()) {
            return null;
        }

        final JsonNode node = mapper.readValue(parser, JsonNode.class);
        nextToken = null; // reset nextToken

        return new SmileEnvelopeEvent(node);
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

        if (firstByte == SMILE_MARKER) {
            return deserialize(pbIn, smileObjectMapper);
        }
        else {
            return deserialize(pbIn, jsonObjectMapper);
        }
    }

    private static List<SmileEnvelopeEvent> deserialize(final InputStream in, final ObjectMapper objectMapper) throws IOException
    {
        final List<SmileEnvelopeEvent> events = new LinkedList<SmileEnvelopeEvent>();

        final JsonParser jp = objectMapper.getJsonFactory().createJsonParser(in);
        final JsonNode root = objectMapper.readValue(jp, JsonNode.class);

        if (root instanceof ArrayNode) {
            final ArrayNode nodes = (ArrayNode) root;
            for (final JsonNode node : nodes) {
                try {
                    final SmileEnvelopeEvent event = new SmileEnvelopeEvent(node);
                    events.add(event);
                }
                catch (IOException e) {
                    log.warn("unable to extract an event. Expect an array of {eventName=<foo>,payload=<bar>} objects.");
                    // keep trying. there might only be one malformed tree?
                }
            }
        }
        else {
            log.warn("unable to extract an event. Expect an array of {eventName=<foo>,payload=<bar>} objects.");
            throw new IOException("root JsonNode must be an ArrayNode");
        }

        jp.close();

        return events;
    }
}
