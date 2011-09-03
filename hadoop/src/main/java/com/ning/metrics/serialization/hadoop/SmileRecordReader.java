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

package com.ning.metrics.serialization.hadoop;

import com.ning.metrics.serialization.event.SmileEnvelopeEvent;
import com.ning.metrics.serialization.smile.SmileEnvelopeEventDeserializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.joda.time.DateTime;

import java.io.IOException;

public class SmileRecordReader extends RecordReader
{
    private SmileEnvelopeEventDeserializer deserializer;
    private long start;
    private long pos;
    private long end;
    private DateTime key = null;
    private SmileEnvelopeEvent value = null;
    private FSDataInputStream fileIn = null;

    public SmileRecordReader()
    {
    }

    /**
     * Called once at initialization.
     *
     * @param genericSplit the split that defines the range of records to read
     * @param context      the information about the task
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException
    {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // Open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(split.getPath());
        if (start != 0) {
            --start;
            fileIn.seek(start);
        }

        this.pos = start;
        deserializer = new SmileEnvelopeEventDeserializer(fileIn, false);
    }

    /**
     * Read the next key, value pair.
     *
     * @return true if a key/value pair was read
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (!deserializer.hasNextEvent()) {
            return false;
        }
        else {
            value = deserializer.getNextEvent();
            key = value.getEventDateTime();
            return true;
        }
    }

    /**
     * Get the current key
     *
     * @return the current key or null if there is no current key
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public Object getCurrentKey() throws IOException, InterruptedException
    {
        return key;
    }

    /**
     * Get the current value.
     *
     * @return the object that was read
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public Object getCurrentValue() throws IOException, InterruptedException
    {
        return value;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return a number between 0.0 and 1.0 that is the fraction of the data read
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException
    {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    /**
     * Close the record reader.
     */
    @Override
    public void close() throws IOException
    {
        if (fileIn != null) {
            fileIn.close();
        }
    }
}
