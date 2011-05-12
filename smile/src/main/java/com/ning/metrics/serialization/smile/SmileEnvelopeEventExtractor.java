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
import org.apache.log4j.Logger;
import org.codehaus.jackson.*;
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

public class SmileEnvelopeEventExtractor
{
    private final static Logger log = Logger.getLogger(SmileEnvelopeEventExtractor.class);
    protected final static SmileFactory smileFactory = new SmileFactory();
    protected final static JsonFactory jsonFactory = new JsonFactory();

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

    /**
     * Given a stream of Json or Smile, create a SmileBucket representation (vector of JsonNodes).
     *
     * @param in data stream (Json or Smile)
     * @return SmileBucket representation
     * @throws IOException generic serialization error
     */
    public static List<SmileEnvelopeEvent> extractEvents(InputStream in) throws IOException
    {
        PushbackInputStream pbIn = new PushbackInputStream(in);

        byte firstByte = (byte) pbIn.read();

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

    /**
     * Given a stream of Json or Smile, create a SmileBucket representation (vector of JsonNodes).
     *
     * @param in           data stream (Json or Smile)
     * @param objectMapper objectMapper with the correct factory (Json or Smile)
     * @return SmileBucket representation
     * @throws IOException generic serialization error
     */
    public static List<SmileEnvelopeEvent> deserialize(InputStream in, ObjectMapper objectMapper) throws IOException
    {
        List<SmileEnvelopeEvent> events = new LinkedList<SmileEnvelopeEvent>();

        JsonParser jp = objectMapper.getJsonFactory().createJsonParser(in);
        JsonNode root = objectMapper.readValue(jp, JsonNode.class);

        if (root instanceof ArrayNode) {
            ArrayNode nodes = (ArrayNode) root;
            for (JsonNode node : nodes) {

                try {
                    SmileEnvelopeEvent event = new SmileEnvelopeEvent(node);
                    events.add(event);
                }
                catch (RuntimeException e) {
                    log.warn("unable to extract an event. Expect an array of {eventName=<foo>,payload=<bar>} objects.");
                }
            }
        }
        else {
            log.warn("unable to extract an event. Expect an array of {eventName=<foo>,payload=<bar>} objects.");
        }

        jp.close();

        return events;
    }
}
