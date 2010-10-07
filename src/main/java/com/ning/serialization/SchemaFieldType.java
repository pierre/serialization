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

package com.ning.serialization;

import org.apache.thrift.protocol.TType;

import java.util.Arrays;

public enum SchemaFieldType
{
    STRING(TType.STRING),
    BOOL(TType.BOOL),
    BYTE(TType.BYTE),
    I16(TType.I16),
    I32(TType.I32),
    IP(TType.I32),
    I64(TType.I64),
    DATE(TType.I64),
    DOUBLE(TType.DOUBLE),;

    private final byte thriftType;

    SchemaFieldType(byte thriftType)
    {
        this.thriftType = thriftType;
    }

    public byte getThriftType()
    {
        return thriftType;
    }

    public SchemaField createSchemaField(String name, short id)
    {
        switch (this) {
            case DATE:
                return new DateSchemaField(name, id);
            case IP:
                return new IpSchemaField(name, id);
            default:
                return new IdentitySchemaField(name, getThriftType(), id);
        }
    }

    @SuppressWarnings("unused")
    public static SchemaField createSchemaField(String name, String type, short id)
    {
        try {
            SchemaFieldType schemaFieldType = SchemaFieldType.valueOf(type.toUpperCase());

            return schemaFieldType.createSchemaField(name, id);
        }
        catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                "Expected type to be one of %s but got %s",
                Arrays.toString(SchemaFieldType.values()).toLowerCase(),
                type
            ));
        }
    }
}
