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

import com.ning.metrics.serialization.event.Granularity;
import com.ning.metrics.serialization.event.SmileBucketEvent;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

public class JsonStreamToSmileBucketEvent
{
    /**
     * Given a stream of Json (smile or plain Json) events of specified type,
     * construct a SmileBucketEvent wrapper. Granularity is set to HOURLY.
     *
     * @param eventName events type (all events in the stream are supposed to be of this type)
     * @param in        Json (smile or plain) stream of events
     * @return Event wrapper around these events
     * @throws IOException generic serialization exception
     */
    public static SmileBucketEvent extractEvent(String eventName, InputStream in) throws IOException
    {
        return extractEvent(eventName, Granularity.HOURLY, in);
    }

    /**
     * Given a stream of Json (smile or plain Json) events of specified type and granularity,
     * construct a SmileBucketEvent wrapper.
     *
     * @param eventName   events type (all events in the stream are supposed to be of this type)
     * @param granularity events granularity
     * @param in          Json (smile or plain) stream of events
     * @return Event wrapper around these events
     * @throws IOException generic serialization exception
     */
    public static SmileBucketEvent extractEvent(String eventName, Granularity granularity, InputStream in) throws IOException
    {
        SmileBucket bucket = SmileBucketDeserializer.deserialize(in);
        return new SmileBucketEvent(eventName, granularity, bucket);
    }
}
