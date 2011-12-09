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

package com.ning.metrics.serialization.thrift.item;

import com.ning.metrics.serialization.thrift.ThriftField;
import com.ning.metrics.serialization.thrift.ThriftFieldImpl;
import com.ning.metrics.serialization.thrift.ThriftFieldListDeserializer;
import com.ning.metrics.serialization.thrift.ThriftFieldListSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestThriftFieldListDeserializer
{
    @Test(groups = "fast")
    public void testHandleMissingFields() throws Exception
    {
        final List<ThriftField> expectedFields = new ArrayList<ThriftField>();
        final ThriftFieldImpl firstField = new ThriftFieldImpl(new StringDataItem("My first value"), (short) 1);
        expectedFields.add(firstField);
        final ThriftFieldImpl thirdField = new ThriftFieldImpl(new StringDataItem("Look! We've skipped 2!"), (short) 3);
        expectedFields.add(thirdField);

        final ThriftFieldListSerializer serializer = new ThriftFieldListSerializer();
        final byte[] payload = serializer.createPayload(expectedFields);

        final ThriftFieldListDeserializer deserializer = new ThriftFieldListDeserializer();
        List<ThriftField> actualFields = deserializer.readPayload(payload);

        Assert.assertEquals(actualFields.size(), 3);
        Assert.assertEquals(actualFields.get(0), firstField);
        Assert.assertEquals(actualFields.get(1), new ThriftFieldImpl(null, (short) 2));
        Assert.assertEquals(actualFields.get(2), thirdField);
    }
}
