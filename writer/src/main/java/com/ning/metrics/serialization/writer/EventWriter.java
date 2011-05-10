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

package com.ning.metrics.serialization.writer;

import com.ning.metrics.serialization.event.Event;

import java.io.IOException;

public interface EventWriter
{
    /**
     * Write an event to disk. This can throw an exception if:
     * <UL>
     * <LI>We cannot open the outputter
     * <LI>If the Java serialization library throws a RuntimeException
     * <LI>Generic IOException from the serialization library
     * </UL>
     * <p/>
     * There is no good reason to put the file in quarantine (rollback) on error. Either the event is bad (RuntimeException)
     * or the write failed, in that case, it's suboptimal to quarantine all events currently in the file.
     *
     * @param event Event to write
     * @throws IOException See above
     */
    public void write(Event event) throws IOException;

    public void commit() throws IOException;

    public void forceCommit() throws IOException;

    public void flush() throws IOException;

    /**
     * Used in case the commit fails (the current output files is moved to quarantined).
     *
     * @throws IOException generic IOException
     */
    public void rollback() throws IOException;
}
