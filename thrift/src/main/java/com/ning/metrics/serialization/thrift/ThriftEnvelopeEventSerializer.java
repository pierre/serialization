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

import com.ning.metrics.serialization.event.Event;
import com.ning.metrics.serialization.event.EventSerializer;
import com.ning.metrics.serialization.event.ThriftEnvelopeEvent;

import java.io.IOException;
import java.io.OutputStream;

public class ThriftEnvelopeEventSerializer implements EventSerializer
{
    OutputStream out;

    @Override
    public void open(OutputStream out) throws IOException
    {
        this.out = out;
    }

    @Override
    public void serialize(Event event) throws IOException
    {
        if (!(event instanceof ThriftEnvelopeEvent)) {
            throw new IllegalArgumentException("ThriftEnvelopeEventSerializer can only serialize ThriftEnvelopeEvents");
        }

        out.write('\n');
        out.write(event.getSerializedEvent());
    }

    @Override
    public void close() throws IOException
    {
        out.close();
    }
}
