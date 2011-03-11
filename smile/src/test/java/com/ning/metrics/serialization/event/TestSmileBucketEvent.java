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

import com.ning.metrics.serialization.smile.SmileBucket;
import com.ning.metrics.serialization.smile.SmileBucketDeserializer;
import com.ning.metrics.serialization.smile.SmileBucketSerializer;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TestSmileBucketEvent
{
    private static final String OUTPUT_DIR = "/hello/world/";

    // { "eventDate": "1242", "granularity": "hourly", "meh": [1,2,3] }
    // { "eventDate": "1243", "granularity": "hourly", "meh": [1,2,3], "bleh": "user-agent" }
    private final static String fileData = new File(new File("").getAbsolutePath(), "src/test/java/com/ning/metrics/serialization/smile/sampleEvents.json").getPath();

    private SmileBucketEvent bucketEvent;
    private SmileBucketEvent bucketEventWithOutputDir;
    private ByteArrayOutputStream finalStream;

    @BeforeTest
    public void setUp() throws IOException
    {
        // We feed plain json but get Smile back
        // FileInputStream inStream = new FileInputStream(fileData);
        // eventsBytes = new byte[inStream.available()];
        // inStream.read(eventsBytes);
        // inStream.close();
        SmileBucket bucket2 = SmileBucketDeserializer.deserialize(new FileInputStream(fileData));
        finalStream = new ByteArrayOutputStream();
        SmileBucketSerializer.serialize(bucket2, finalStream);

        SmileBucket bucket = SmileBucketDeserializer.deserialize(new FileInputStream(fileData));
        bucketEvent = new SmileBucketEvent("testEvent", Granularity.HOURLY, bucket);
        bucketEventWithOutputDir = new SmileBucketEvent("testEvent", Granularity.HOURLY, OUTPUT_DIR, bucket);
    }

    @Test(groups = "fast")
    public void testNumberOfEvents() throws Exception
    {
        Assert.assertEquals(bucketEvent.getNumberOfEvent(), 2);
    }

    @Test(groups = "fast")
    public void testGetData() throws Exception
    {
        byte[] bytes = ((ByteArrayOutputStream) bucketEvent.getData()).toByteArray();

        Assert.assertEquals(bytes, finalStream.toByteArray());

        // Test we get Smile back
        Assert.assertEquals(bytes[0], ':');
        Assert.assertEquals(bytes[1], ')');
        Assert.assertEquals(bytes[2], '\n');
    }

    @Test(groups = "fast")
    public void testGetSerializedEvent() throws Exception
    {
        Assert.assertEquals(bucketEvent.getSerializedEvent(), finalStream.toByteArray());
    }

    @Test(groups = "fast")
    public void testReadWriteExternal() throws Exception
    {
        SmileBucketEvent event = doTestsRW(bucketEvent);
        try {
            event.getOutputDir("");
            Assert.fail();
        }
        catch (RuntimeException e) {
            // Hasn't been set, should trigger an exception
            Assert.assertTrue(true);
        }
    }

    @Test(groups = "fast")
    public void testReadWriteExternalWithOutputDir() throws Exception
    {
        SmileBucketEvent event = doTestsRW(bucketEventWithOutputDir);
        Assert.assertEquals(event.getOutputDir(""), OUTPUT_DIR);
    }

    private SmileBucketEvent doTestsRW(SmileBucketEvent bucket) throws IOException, ClassNotFoundException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(out);
        bucket.writeExternal(outputStream);
        outputStream.close();

        SmileBucketEvent bucketEvent2 = new SmileBucketEvent();
        bucketEvent2.readExternal(new ObjectInputStream(new ByteArrayInputStream(out.toByteArray())));

        Assert.assertEquals(bucketEvent2.getName(), bucket.getName());
        Assert.assertEquals(bucketEvent2.getGranularity(), bucket.getGranularity());
        Assert.assertEquals(bucketEvent2.getSerializedEvent(), bucket.getSerializedEvent());

        Assert.assertEquals(bucketEvent2.getNumberOfEvent(), bucket.getNumberOfEvent());

        return bucketEvent2;
    }
}
