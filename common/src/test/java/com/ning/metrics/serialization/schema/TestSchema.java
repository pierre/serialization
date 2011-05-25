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

package com.ning.metrics.serialization.schema;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestSchema
{
    private static final String EVENT_NAME = "FuuEvent";
    private static final String EVENT_FIELD1_NAME = "FuuField1";
    private static final String EVENT_FIELD1_TYPE = "STRING";
    private static final String EVENT_FIELD2_NAME = "FuuField2";
    private static final String EVENT_FIELD2_TYPE = "DATE";
    private static final String EVENT_FIELD3_NAME = "FuuField3";
    private static final String EVENT_FIELD3_TYPE = "BOOLEAN";


    @Test(groups = "fast")
    public void testCreateSchema() throws Exception
    {
        final Schema schema = new Schema(EVENT_NAME);

        Assert.assertEquals(schema.getName(), EVENT_NAME);

        // Un-ordered on purpose
        schema.addSchemaField(SchemaFieldType.createSchemaField(EVENT_FIELD1_NAME, EVENT_FIELD1_TYPE, (short) 1));
        schema.addSchemaField(SchemaFieldType.createSchemaField(EVENT_FIELD3_NAME, EVENT_FIELD3_TYPE, (short) 3));
        schema.addSchemaField(SchemaFieldType.createSchemaField(EVENT_FIELD2_NAME, EVENT_FIELD2_TYPE, (short) 2));

        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD1_NAME).getName(), EVENT_FIELD1_NAME);
        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD2_NAME).getName(), EVENT_FIELD2_NAME);
        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD3_NAME).getName(), EVENT_FIELD3_NAME);

        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD1_NAME).getType(), SchemaFieldType.valueOf(EVENT_FIELD1_TYPE));
        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD2_NAME).getType(), SchemaFieldType.valueOf(EVENT_FIELD2_TYPE));
        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD3_NAME).getType(), SchemaFieldType.valueOf(EVENT_FIELD3_TYPE));

        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD1_NAME).getId(), (short) 1);
        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD2_NAME).getId(), (short) 2);
        Assert.assertEquals(schema.getFieldByName(EVENT_FIELD3_NAME).getId(), (short) 3);

        Assert.assertEquals(schema.getFieldByPosition((short) 1), schema.getFieldByName(EVENT_FIELD1_NAME));
        Assert.assertEquals(schema.getFieldByPosition((short) 2), schema.getFieldByName(EVENT_FIELD2_NAME));
        Assert.assertEquals(schema.getFieldByPosition((short) 3), schema.getFieldByName(EVENT_FIELD3_NAME));

        // Test ordering
        final List<SchemaField> field = schema.getSchema();
        Assert.assertEquals(field.get(0), schema.getFieldByName(EVENT_FIELD1_NAME));
        Assert.assertEquals(field.get(2), schema.getFieldByName(EVENT_FIELD3_NAME));
        Assert.assertEquals(field.get(1), schema.getFieldByName(EVENT_FIELD2_NAME));
    }
}
