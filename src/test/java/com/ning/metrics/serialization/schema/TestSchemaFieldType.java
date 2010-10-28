package com.ning.metrics.serialization.schema;

import org.apache.thrift.protocol.TType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSchemaFieldType
{
    private static final String FIELD_NAME = "FuuFieldForFuuEvent";
    private static final short FIELD_ID = (short) 1;

    @Test(groups = "fast")
    public void testCreateSchemaFieldBoolean() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "BOOLEAN", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.BOOLEAN);
        Assert.assertEquals(testField.getType().getThriftType(), TType.BOOL);
        Assert.assertEquals(testField.getType().getSmileType(), Boolean.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldByte() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "BYTE", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.BYTE);
        Assert.assertEquals(testField.getType().getThriftType(), TType.BYTE);
        Assert.assertEquals(testField.getType().getSmileType(), Byte.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldShort() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "SHORT", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.SHORT);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I16);
        Assert.assertEquals(testField.getType().getSmileType(), Short.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldInteger() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "INTEGER", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.INTEGER);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I32);
        Assert.assertEquals(testField.getType().getSmileType(), Integer.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldLong() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "LONG", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.LONG);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I64);
        Assert.assertEquals(testField.getType().getSmileType(), Long.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldDouble() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "DOUBLE", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.DOUBLE);
        Assert.assertEquals(testField.getType().getThriftType(), TType.DOUBLE);
        Assert.assertEquals(testField.getType().getSmileType(), Double.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldString() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "STRING", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.STRING);
        Assert.assertEquals(testField.getType().getThriftType(), TType.STRING);
        Assert.assertEquals(testField.getType().getSmileType(), String.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldDate() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "DATE", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.DATE);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I64);
        Assert.assertEquals(testField.getType().getSmileType(), Long.class);
    }

    @Test(groups = "fast")
    public void testCreateSchemaFieldIp() throws Exception
    {
        SchemaField testField = SchemaFieldType.createSchemaField(FIELD_NAME, "IP", FIELD_ID);

        Assert.assertEquals(testField.getName(), FIELD_NAME);
        Assert.assertEquals(testField.getId(), FIELD_ID);

        Assert.assertEquals(testField.getType(), SchemaFieldType.IP);
        Assert.assertEquals(testField.getType().getThriftType(), TType.I32);
        Assert.assertEquals(testField.getType().getSmileType(), Integer.class);
    }
}
