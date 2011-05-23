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
import com.ning.metrics.serialization.event.EventSerializer;
import com.ning.metrics.serialization.event.SmileEnvelopeEvent;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;

import java.io.IOException;
import java.io.OutputStream;

public class SmileEnvelopeEventSerializer implements EventSerializer<SmileEnvelopeEvent>
{
    JsonGenerator jsonGenerator;
    boolean plainJson;

    protected final static SmileFactory smileFactory = new SmileFactory();
    protected final static JsonFactory jsonFactory = new JsonFactory();
    
    static {
        // yes, full 'compression' by checking for repeating names, short string values:
        smileFactory.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        smileFactory.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        // and for now let's not mandate header for input
        smileFactory.configure(SmileParser.Feature.REQUIRE_HEADER, false);
    }

    public SmileEnvelopeEventSerializer(boolean plainJson) {
        this.plainJson = plainJson;
    }

    @Override
    public void open(OutputStream out) throws IOException
    {
        if (plainJson) {
            jsonGenerator = jsonFactory.createJsonGenerator(out, JsonEncoding.UTF8);
        }
        else {
            jsonGenerator = smileFactory.createJsonGenerator(out, JsonEncoding.UTF8);
        }
        jsonGenerator.writeStartArray();
    }

    @Override
    public void serialize(SmileEnvelopeEvent event) throws IOException
    {
        event.setPlainJson(plainJson);
        event.writeToJsonGenerator(jsonGenerator);
    }

    @Override
    public void close() throws IOException
    {
        jsonGenerator.writeEndArray();
        jsonGenerator.close();
    }
}
