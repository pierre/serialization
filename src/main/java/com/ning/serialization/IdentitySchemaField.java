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

public class IdentitySchemaField extends AbstractSchemaField implements SchemaField
{
    public IdentitySchemaField(String name, byte type, short id)
    {
        super(name, getSchemaFieldType(type), id);
    }

    @Override
    public DataItem convert(DataItem dataItem)
    {
        return dataItem;
    }

    @Override
    public DataItem invert(DataItem dataItem)
    {
        return DataItemConverter.convert(dataItem, DataItemTypes.fromTType(getType().getThriftType()));
    }

    private static SchemaFieldType getSchemaFieldType(byte thriftType)
    {
        switch (thriftType) {
            case TType.BOOL:
                return SchemaFieldType.BOOL;
            case TType.BYTE:
                return SchemaFieldType.BYTE;
            case TType.DOUBLE:
                return SchemaFieldType.DOUBLE;
            case TType.I16:
                return SchemaFieldType.I16;
            case TType.I32:
                return SchemaFieldType.I32;
            case TType.I64:
                return SchemaFieldType.I64;
            case TType.STRING:
                return SchemaFieldType.STRING;
            default:
                throw new IllegalArgumentException("Invalid thrift type for schema field: " + thriftType);
        }
    }
}