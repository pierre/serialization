/*
 * Copyright 2011 Ning, Inc.
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

import java.io.IOException;
import java.io.InputStream;

public class SmileBucketDeserializer
{
    protected final ObjectMapper objectMapper;
    private JsonParser jp;
    private final SmileBucket bucket = new SmileBucket();

    public SmileBucketDeserializer()
    {
        this(new JsonFactory());
    }

    public SmileBucketDeserializer(JsonFactory jsonFactory)
    {
        objectMapper = new ObjectMapper(jsonFactory);
        jsonFactory.setCodec(objectMapper);
    }

    public void open(InputStream in) throws IOException
    {
        jp = objectMapper.getJsonFactory().createJsonParser(in);
    }

    public SmileBucket deserialize() throws IOException
    {
        while (jp.nextToken() != null) {
            bucket.add(objectMapper.readValue(jp, JsonNode.class));
        }

        return bucket;
    }

    public void close() throws IOException
    {
        jp.close();
    }
}
