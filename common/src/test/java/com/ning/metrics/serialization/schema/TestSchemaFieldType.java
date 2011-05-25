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

public class TestSchemaFieldType
{
    private static final String FIELD_NAME = "FuuFieldForFuuEvent";
    private static final short FIELD_ID = (short) 1;

    @Test(groups = "fast")
    public void testCreateSchemaFieldBoolean() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "BOOLEAN", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.BOOLEAN);
        Assert.assertEquals(testField.getType().getThriftType(), TType.BOOL);
        Assert.assertEquals(testField.getType().getSmileType(), Boolean.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldByte() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "BYTE", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.BYTE);
        Assert.assertEquals(testField.getType().getThriftType(), TType.BYTE);
        Assert.assertEquals(testField.getType().getSmileType(), Byte.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldShort() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "SHORT", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.SHORT);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I16);
        Assert.assertEquals(testField.getType().getSmileType(), Short.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldInteger() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "INTEGER", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.INTEGER);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I32);
        Assert.assertEquals(testField.getType().getSmileType(), Integer.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldLong() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "LONG", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.LONG);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I64);
        Assert.assertEquals(testField.getType().getSmileType(), Long.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldDouble() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "DOUBLE", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.DOUBLE);
        Assert.assertEquals(testField.getType().getThriftType(), TType.DOUBLE);
        Assert.assertEquals(testField.getType().getSmileType(), Double.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldString() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "STRING", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.STRING);
        Assert.assertEquals(testField.getType().getThriftType(), TType.STRING);
        Assert.assertEquals(testField.getType().getSmileType(), String.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldDate() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "DATE", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.DATE);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I64);
        Assert.assertEquals(testField.getType().getSmileType(), Long.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldIp() throws Exception
    {
        final SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "IP", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.IP);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I32);
        Assert.assertEquals(testField.getType().getSmileType(), Integer.class);
    }
}
