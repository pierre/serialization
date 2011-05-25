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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class SmileEnvelopeEventsToSmileBucketEvents
{
    /**
     * Given a list of SmileEnvelopeEvents, create the equivalent SmileBucketEvents.
     * This utility function groups the events by name and output path.
     * <p/>
     * We return a list here as there may be multiple event types in the list.
     * TODO This assumes that all events of same type share the same granularity. This is generally
     * true in practice but isn't explicitly enforced generally (but here).
     *
     * @param events Json events to extracts
     * @return a list of SmileBucketEvent, fully populated and ready to be shipped to the collector
     */
    public static Collection<SmileBucketEvent> extractEvents(final Iterable<SmileEnvelopeEvent> events)
    {
        // MyEvent => {
        //          /2010/01/02/10/00 => SmileBucketEvent
        //          /2010/01/02/10/01 => SmileBucketEvent
        // }
        final HashMap<String, HashMap<String, SmileBucketEvent>> finalEvents = new HashMap<String, HashMap<String, SmileBucketEvent>>();

        for (final SmileEnvelopeEvent envelope : events) {
            String eventName = envelope.getName();

            // New event type?
            if (finalEvents.get(eventName) == null) {
                finalEvents.put(eventName, new HashMap<String, SmileBucketEvent>());
            }
            HashMap<String, SmileBucketEvent> eventsByPath = finalEvents.get(eventName);

            // New path?
            // TODO This assumes same granularity for same event types
            String outputDir = envelope.getOutputDir("");
            if (eventsByPath.get(outputDir) == null) {
                eventsByPath.put(outputDir, new SmileBucketEvent(eventName, envelope.getGranularity(), envelope.getOutputDir(""), new SmileBucket()));
            }
            SmileBucketEvent event = eventsByPath.get(outputDir);

            event.getBucket().add((JsonNode) envelope.getData());
        }

        final Collection<SmileBucketEvent> results = new ArrayList<SmileBucketEvent>();
        for (final HashMap<String, SmileBucketEvent> finalEventsByPath : finalEvents.values()) {
            results.addAll(finalEventsByPath.values());
        }

        return results;
    }
}
