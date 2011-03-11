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

import org.codehaus.jackson.JsonNode;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public class TestSmileBucketDeserializer
{
    private InputStream in;

    //{ "foo": "bar", "baz": 1242, "meh": [1,2,3] }
    //{ "foo": "bar2", "baz": 4212 }
    private final static String fileData = new File(new File("").getAbsolutePath(), "src/test/java/com/ning/metrics/serialization/smile/sampleData.json").getPath();

    @BeforeTest
    public void setUp() throws FileNotFoundException
    {
        in = new FileInputStream(fileData);
    }

    @Test
    public void testDeserialize() throws Exception
    {
        SmileBucket bucket = SmileBucketDeserializer.deserialize(in);

        Assert.assertEquals(bucket.size(), 2);

        JsonNode json = bucket.get(0);
        Assert.assertEquals(json.path("foo").getValueAsText(), "bar");
        Assert.assertEquals(json.path("baz").getValueAsInt(), 1242);
        Assert.assertTrue(json.path("meh").isArray());
        Assert.assertEquals(json.path("meh").size(), 3);
        Assert.assertEquals(json.path("meh").get(1).getValueAsInt(), 2);

        Assert.assertEquals(bucket.get(1).toString(), "{\"foo\":\"bar2\",\"baz\":4212}");
    }
}
