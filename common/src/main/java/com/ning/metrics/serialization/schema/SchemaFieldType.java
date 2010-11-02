/*
 * Copyright 2010 Ning, Inc.
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

import java.util.Arrays;

/**
 * Hack - import the org.apache.thrift.protocol.TType class constants here
 * to avoid a dependency on org.apache.thrift:thrift.
 *
 * These should hopefully not change often.
 */
final class TType
{
    public static final byte STOP = 0;
    public static final byte VOID = 1;
    public static final byte BOOL = 2;
    public static final byte BYTE = 3;
    public static final byte DOUBLE = 4;
    public static final byte I16 = 6;
    public static final byte I32 = 8;
    public static final byte I64 = 10;
    public static final byte STRING = 11;
    public static final byte STRUCT = 12;
    public static final byte MAP = 13;
    public static final byte SET = 14;
    public static final byte LIST = 15;
    public static final byte ENUM = 16;
}

/**
 * The SchemaFieldType ties the high level data representation
 * (e.g. date, IP) to the underlying protocol types (e.g. i64, ...).
 */
public enum SchemaFieldType
{
    BOOLEAN(TType.BOOL, Boolean.class),
    BYTE(TType.BYTE, Byte.class),
    SHORT(TType.I16, Short.class),
    INTEGER(TType.I32, Integer.class),
    LONG(TType.I64, Long.class),
    DOUBLE(TType.DOUBLE, Double.class),
    STRING(TType.STRING, String.class),
    DATE(TType.I64, Long.class),
    IP(TType.I32, Integer.class);

    private final byte thriftType;
    private final Class<?> smileType;

    SchemaFieldType(byte thriftType, Class<?> smileType)
    {
        this.thriftType = thriftType;
        this.smileType = smileType;
    }

    public byte getThriftType()
    {
        return thriftType;
    }

    public Class<?> getSmileType()
    {
        return smileType;
    }

    public SchemaField createSchemaField(String name, short id)
    {
        switch (this) {
            case DATE:
                return new DateSchemaField(name, id);
            case IP:
                return new IpSchemaField(name, id);
            default:
                return new IdentitySchemaField(name, this, id);
        }
    }

    /**
     * Used primarily by Goodwill to create schemata.
     *
     * @param name name of the field
     * @param type plain text name of the field
     * @param id   position in the schema (should be unique by schema)
     * @return newly created SchemaField
     */
    @SuppressWarnings("unused")
    public static SchemaField createSchemaField(String name, String type, short id)
    {
        try {
            SchemaFieldType schemaFieldType = valueOf(type.toUpperCase());
            return schemaFieldType.createSchemaField(name, id);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                "Expected type to be one of %s but got %s",
                Arrays.toString(values()).toLowerCase(),
                type
            ));
        }
    }
}
