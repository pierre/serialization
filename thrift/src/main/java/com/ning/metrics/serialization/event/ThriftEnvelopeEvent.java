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

import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftEnvelopeDeserializer;
import com.ning.metrics.serialization.thrift.ThriftEnvelopeSerializer;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class ThriftEnvelopeEvent implements Event
{
    private DateTime eventDateTime;
    private ThriftEnvelope thriftEnvelope;
    private Granularity granularity;
    private transient byte[] serializedBytes;
    private final transient ThriftEnvelopeSerializer serializer = new ThriftEnvelopeSerializer();
    private final transient ThriftEnvelopeDeserializer deserializer;

    /**
     * Public no-arg constructor, for deserialization
     */
    public ThriftEnvelopeEvent()
    {
        this(null, null, null);
    }

    public ThriftEnvelopeEvent(final DateTime eventDateTime, final ThriftEnvelope thriftEnvelope)
    {
        this(eventDateTime, thriftEnvelope, Granularity.HOURLY);
    }

    public ThriftEnvelopeEvent(final DateTime eventDateTime, final ThriftEnvelope thriftEnvelope, final Granularity granularity)
    {
        this.eventDateTime = eventDateTime;
        this.thriftEnvelope = thriftEnvelope;
        this.granularity = granularity;

        deserializer = new ThriftEnvelopeDeserializer();
    }

    /**
     * Deserialize one event from the specified stream.
     * This expects the stream to be open and won't close it. The specified deserializer should take care of this.
     *
     * @param in           inputstream to read
     * @param deserializer deserializer responsible to open/close the stream
     * @throws IOException generic I/O Exception
     */
    public ThriftEnvelopeEvent(final InputStream in, final ThriftEnvelopeDeserializer deserializer) throws IOException
    {
        this.deserializer = deserializer;
        deserializeFromStream(in);
    }

    @Override
    public DateTime getEventDateTime()
    {
        return eventDateTime;
    }

    @Override
    public String getName()
    {
        return thriftEnvelope.getTypeName();
    }

    @Override
    public Granularity getGranularity()
    {
        return granularity;
    }

    @Override
    public String getVersion()
    {
        return thriftEnvelope.getVersion();
    }

    @Override
    public String getOutputDir(final String prefix)
    {
        final GranularityPathMapper pathMapper = new GranularityPathMapper(String.format("%s/%s", prefix, thriftEnvelope.getTypeName()), granularity);

        return pathMapper.getPathForDateTime(getEventDateTime());
    }

    @Override
    public Object getData()
    {
        return thriftEnvelope;
    }

    @Override
    public byte[] getSerializedEvent()
    {
        try {
            toBytes();
            return serializedBytes;
        }
        catch (IOException e) {
            return null;
        }
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException
    {
        final int numBytes = in.readInt();
        final byte[] fullPayload = new byte[numBytes];
        in.readFully(fullPayload);

        final ByteArrayInputStream inputBuffer = new ByteArrayInputStream(fullPayload, 0, fullPayload.length);

        deserializer.open(inputBuffer);
        deserializeFromStream(inputBuffer);
        deserializer.close();
    }

    /**
     * Given an InputStream, extract the eventDateTime, granularity and thriftEnvelope to build
     * the ThriftEnvelopeEvent.
     * This method expects the stream to be open and won't close it for you.
     *
     * @param in InputStream to read
     * @throws IOException generic I/O Exception
     */
    private void deserializeFromStream(final InputStream in) throws IOException
    {
        final byte[] dateTimeBytes = new byte[8];
        in.read(dateTimeBytes, 0, 8);
        eventDateTime = new DateTime(ByteBuffer.wrap(dateTimeBytes).getLong(0));

        final byte[] sizeGranularityInBytes = new byte[4];
        in.read(sizeGranularityInBytes, 0, 4);
        final byte[] granularityBytes = new byte[ByteBuffer.wrap(sizeGranularityInBytes).getInt(0)];
        in.read(granularityBytes, 0, granularityBytes.length);
        granularity = Granularity.valueOf(new String(granularityBytes, Charset.forName("UTF-8")));

        thriftEnvelope = deserializer.deserialize(null);
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException
    {
        toBytes();
        out.writeInt(serializedBytes.length);
        out.write(serializedBytes);
    }

    private void toBytes() throws IOException
    {
        if (serializedBytes == null) {
            final ByteArrayOutputStream outputBuffer = new ByteArrayOutputStream();

            outputBuffer.write(ByteBuffer.allocate(8).putLong(eventDateTime.getMillis()).array());

            final byte[] granularityBytes = granularity.name().getBytes(Charset.forName("UTF-8"));
            outputBuffer.write(ByteBuffer.allocate(4).putInt(granularityBytes.length).array());
            outputBuffer.write(ByteBuffer.allocate(granularityBytes.length).put(granularityBytes).array());

            serializer.open(outputBuffer);
            serializer.serialize(thriftEnvelope);
            serializer.close();

            serializedBytes = outputBuffer.toByteArray();
        }
    }

    @Override
    public String toString()
    {
        return "ThriftEnvelopeEvent{" +
            "eventDateTime=" + eventDateTime +
            ", thriftEnvelope=" + thriftEnvelope +
            ", granularity=" + granularity +
            '}';
    }
}
