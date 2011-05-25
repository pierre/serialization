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

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class SmileBucketDeserializer
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

    /**
     * Given a stream of Json or Smile, create a SmileBucket representation (vector of JsonNodes).
     *
     * @param in data stream (Json or Smile)
     * @return SmileBucket representation
     * @throws IOException generic serialization error
     */
    public static SmileBucket deserialize(final InputStream in) throws IOException
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

    /**
     * Given a stream of Json or Smile, create a SmileBucket representation (vector of JsonNodes).
     *
     * @param in           data stream (Json or Smile)
     * @param objectMapper objectMapper with the correct factory (Json or Smile)
     * @return SmileBucket representation
     * @throws IOException generic serialization error
     */
    public static SmileBucket deserialize(final InputStream in, final ObjectMapper objectMapper) throws IOException
    {
        final SmileBucket bucket = new SmileBucket();

        final JsonParser jp = objectMapper.getJsonFactory().createJsonParser(in);
        final JsonNode root = objectMapper.readValue(jp, JsonNode.class);

        if (root instanceof ArrayNode) {
            final ArrayNode nodes = (ArrayNode) root;
            for (final JsonNode node : nodes) {
                bucket.add(node);
            }
        }
        else if (root != null) { // Don't add null values in the bucket
            bucket.add(root);
            while (jp.nextToken() != null) {
                bucket.add(objectMapper.readValue(jp, JsonNode.class));
            }
        }

        jp.close();

        return bucket;
    }
}
