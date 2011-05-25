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

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;

import java.io.IOException;
import java.io.OutputStream;

public class SmileBucketSerializer
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


    public static void serialize(final SmileBucket bucket, final OutputStream outStream) throws IOException
    {
        serializeSmile(bucket, outStream);
    }

    public static void serializeSmile(final SmileBucket bucket, final OutputStream outStream) throws IOException
    {
        serialize(bucket, outStream, smileObjectMapper);
    }

    public static void serializeJson(final SmileBucket bucket, final OutputStream outStream) throws IOException
    {
        serialize(bucket, outStream, jsonObjectMapper);
    }

    public static void serialize(final SmileBucket bucket, final OutputStream outStream, final ObjectMapper objectMapper) throws IOException
    {
        final JsonGenerator gen = objectMapper.getJsonFactory().createJsonGenerator(outStream, JsonEncoding.UTF8);
        objectMapper.writeValue(gen, bucket);
        gen.flush();
        gen.close();
    }
}
