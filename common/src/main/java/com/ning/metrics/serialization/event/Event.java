/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.serialization.event;

import org.joda.time.DateTime;

import java.io.Externalizable;

public interface Event extends Externalizable
{
    public DateTime getEventDateTime();

    public String getName();

    public Granularity getGranularity();

    public String getVersion();

    public String getOutputDir(String prefix);

    /**
     * Object representing the payload. This is used for instance when writing to Hadoop in the collector
     * (object serialized into sequence files).
     *
     * @return Object representing the data (ThriftEnvelope, ...)
     */
    public Object getData();

    /**
     * Serialize an event to a byte array. This is used for instance when sending data on the wire.
     * This method is optional, methods relying on this call should handle gracefully null.
     *
     * @return byte array representation of an event, can return null
     */
    public byte[] getSerializedEvent();
}
