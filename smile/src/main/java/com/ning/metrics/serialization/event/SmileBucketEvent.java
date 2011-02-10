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

package com.ning.metrics.serialization.event;

import com.ning.metrics.serialization.smile.SmileBucket;
import com.ning.metrics.serialization.smile.SmileBucketDeserializer;
import com.ning.metrics.serialization.smile.SmileBucketSerializer;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;

/**
 * A meta-event wrapping a collector of Smile events of the same type and same granularity.
 * This is used for serializing, to leverage compression (back-references) that Smile offers.
 */
public class SmileBucketEvent implements Event
{
    private SmileBucket bucket;
    private Granularity granularity;
    private String eventName;

    private ByteArrayOutputStream eventStream = null;
    private static final Charset CHARSET = Charset.forName("UTF-8");

    public SmileBucketEvent(String eventName, Granularity granularity, SmileBucket bucket)
    {
        this.eventName = eventName;
        this.granularity = granularity;
        this.bucket = bucket;
    }

    @Deprecated
    public SmileBucketEvent()
    {
    }

    @Override
    public DateTime getEventDateTime()
    {
        throw new RuntimeException("Events in SmileBucket have different timestamps!");
    }

    @Override
    public String getName()
    {
        return eventName;
    }

    @Override
    public Granularity getGranularity()
    {
        return granularity;
    }

    @Override
    public String getVersion()
    {
        return null;
    }

    @Override
    public String getOutputDir(String prefix)
    {
        GranularityPathMapper pathMapper = new GranularityPathMapper(String.format("%s/%s", prefix, eventName), granularity);

        return pathMapper.getPathForDateTime(getEventDateTime());
    }

    /**
     * @return Object representing the data, compressed Smile version of the events
     */
    @Override
    public Object getData()
    {
        if (eventStream == null) {
            eventStream = new ByteArrayOutputStream();

            try {
                SmileBucketSerializer.serialize(bucket, eventStream);
            }
            catch (IOException e) {
                return null;
            }
        }

        return eventStream;
    }

    /**
     * Serialize an event to a byte array.
     * This method is optional, methods relying on this call should handle gracefully null.
     *
     * @return byte array representation of an event, can return null
     */
    @Override
    public byte[] getSerializedEvent()
    {
        return ((ByteArrayOutputStream) getData()).toByteArray();
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException
    {
        int eventNameLength = eventName.length();
        objectOutput.writeInt(eventNameLength);
        objectOutput.write(eventName.getBytes(CHARSET));

        int granularityLength = granularity.toString().length();
        objectOutput.writeInt(granularityLength);
        objectOutput.write(granularity.toString().getBytes(CHARSET));

        byte[] data = getSerializedEvent();
        int dataLen = data.length;

        objectOutput.writeInt(dataLen);
        objectOutput.write(data);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException
    {
        int eventNameLength = objectInput.readInt();
        byte[] eventNameData = new byte[eventNameLength];
        objectInput.readFully(eventNameData);
        eventName = new String(eventNameData, CHARSET);

        int granularityLength = objectInput.readInt();
        byte[] granularityData = new byte[granularityLength];
        objectInput.readFully(granularityData);
        try {
            granularity = Granularity.valueOf(new String(granularityData, CHARSET));
        }
        catch (IllegalArgumentException e) {
            granularity = Granularity.HOURLY;
        }

        int dataLen = objectInput.readInt();
        byte[] data = new byte[dataLen];
        objectInput.readFully(data);

        bucket = SmileBucketDeserializer.deserialize(new ByteArrayInputStream(data));
    }

    public int getNumberOfEvent()
    {
        return bucket.size();
    }
}
