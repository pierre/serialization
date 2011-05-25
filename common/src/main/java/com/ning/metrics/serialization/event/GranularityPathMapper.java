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

import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInterval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

class GranularityPathMapper implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final String prefix;
    private final Granularity granularity;

    public GranularityPathMapper(final String prefix, final Granularity granularity)
    {
        this.prefix = prefix;
        this.granularity = granularity;
    }

    public String getPrefix()
    {
        return prefix;
    }

    public String getRootPath()
    {
        return prefix;
    }

    public Granularity getGranularity()
    {
        return granularity;
    }

    public Collection<String> getPathsForInterval(final ReadableInterval interval)
    {
        final Collection<String> paths = new ArrayList<String>();

        granularity.stepThroughInterval(interval, new Granularity.Callback<RuntimeException>()
        {
            public void step(final ReadableInterval stepInterval) throws RuntimeException
            {
                paths.add(getPathForDateTime(stepInterval.getStart()));
            }
        });

        return paths;
    }

    public String getPathForDateTime(final ReadableDateTime dateTime)
    {
        return String.format("%s/%s", prefix, granularity.getRelativePathFor(dateTime));
    }
}
