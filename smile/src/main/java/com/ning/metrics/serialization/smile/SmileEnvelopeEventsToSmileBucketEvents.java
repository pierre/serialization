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

import com.ning.metrics.serialization.event.SmileBucketEvent;
import com.ning.metrics.serialization.event.SmileEnvelopeEvent;
import org.codehaus.jackson.JsonNode;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class SmileEnvelopeEventsToSmileBucketEvents
{
    /**
     * Given a list of SmileEnvelopeEvents, create the equivalent SmileBucketEvents.
     * We return a list here as there may be multiple event types in the list.
     * TODO This assumes that all events of same type share the same granularity. This is generally
     * true in practice but isn't explicitly enforced generally (but here).
     *
     * @param events Json events to extracts
     * @return a list of SmileBucketEvent, fully populated
     */
    public static Collection<SmileBucketEvent> extractEvents(List<SmileEnvelopeEvent> events)
    {
        HashMap<String, SmileBucketEvent> finalEvents = new HashMap<String, SmileBucketEvent>();

        String eventName;
        SmileBucketEvent event;
        for (SmileEnvelopeEvent envelope : events) {
            eventName = envelope.getName();

            // New event type?
            if (finalEvents.get(eventName) == null) {
                // TODO This assumes same granularity for same event types
                finalEvents.put(eventName, new SmileBucketEvent(eventName, envelope.getGranularity(), new SmileBucket()));
            }

            event = finalEvents.get(eventName);
            event.getBucket().add((JsonNode) envelope.getData());
        }

        return finalEvents.values();
    }
}
