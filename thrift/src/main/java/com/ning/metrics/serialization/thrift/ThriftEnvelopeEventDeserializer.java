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
package com.ning.metrics.serialization.thrift;

import com.ning.metrics.serialization.event.EventDeserializer;
import com.ning.metrics.serialization.event.ThriftEnvelopeEvent;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

public class ThriftEnvelopeEventDeserializer implements EventDeserializer
{
    private final PushbackInputStream in;

    // cannot successfully parse any events after failing once
    private boolean hasFailed = false;

    public ThriftEnvelopeEventDeserializer(InputStream in)
    {
        this.in = new PushbackInputStream(in);
    }

    public boolean hasNextEvent()
    {
        if (hasFailed) {
            return false;
        }

        try {
            final byte separator = (byte) in.read();
            boolean hasEvent = separator == '\n';
            in.unread(separator);
            return hasEvent;
        }
        catch (IOException e) {
            hasFailed = true;
            return false; // EOF?
        }
    }

    public ThriftEnvelopeEvent getNextEvent() throws IOException
    {
        try {
            if (hasNextEvent() && (byte) in.read() == '\n') {
                return new ThriftEnvelopeEvent(in);
            }
            else {
                throw new IOException("Couldn't find any more events in the stream");
            }
        }
        catch (IOException e) {
            hasFailed = true;
            throw e;
        }
    }
}
