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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class Events
{
    /**
     * Given a file written by the serialization-writer library, extract all events.
     * This assumes the file was written using ObjectOutputStream.
     *
     * @param file file to deserialize
     * @return events contained in the file
     * @throws java.io.IOException    generic IOException
     * @throws ClassNotFoundException if the underlying Event class is not in the classpath
     */
    public static List<Event> fromFile(final File file) throws IOException, ClassNotFoundException
    {
        return fromInputStream(new FileInputStream(file));
    }

    /**
     * Given a stream written by the serialization-writer library, extract all events.
     * This assumes the file was written using ObjectOutputStream.
     *
     * @param stream stream to deserialize
     * @return events contained in the file
     * @throws java.io.IOException    generic IOException
     * @throws ClassNotFoundException if the underlying Event class is not in the classpath
     */
    public static List<Event> fromInputStream(final InputStream stream) throws IOException, ClassNotFoundException
    {
        return fromObjectInputStream(new ObjectInputStream(stream));
    }

    /**
     * Given a stream written by the serialization-writer library, extract all events.
     * This assumes the file was written using ObjectOutputStream.
     *
     * @param objectInputStream stream to deserialize
     * @return events contained in the file
     * @throws java.io.IOException    generic IOException
     * @throws ClassNotFoundException if the underlying Event class is not in the classpath
     */
    public static List<Event> fromObjectInputStream(ObjectInputStream objectInputStream) throws IOException, ClassNotFoundException
    {
        final ArrayList<Event> events = new ArrayList<Event>();

        while (objectInputStream.read() != -1) {
            Event e = (Event) objectInputStream.readObject();
            events.add(e);
        }
        objectInputStream.close();

        return events;
    }
}
