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

package com.ning.metrics.serialization.thrift.item;

import org.apache.thrift.protocol.TType;

public enum DataItemTypes
{
    BOOLEAN(TType.BOOL),
    BYTE(TType.BYTE),
    SHORT(TType.I16),
    INTEGER(TType.I32),
    LONG(TType.I64),
    DOUBLE(TType.DOUBLE),
    STRING(TType.STRING),;


    private byte tType;

    DataItemTypes(byte tType)
    {
        this.tType = tType;
    }

    public byte getTType()
    {
        return tType;
    }

    public static DataItemTypes fromTType(byte tType)
    {
        switch (tType) {
            case TType.BOOL:
                return BOOLEAN;
            case TType.BYTE:
                return BYTE;
            case TType.I16:
                return SHORT;
            case TType.I32:
                return INTEGER;
            case TType.I64:
                return LONG;
            case TType.DOUBLE:
                return DOUBLE;
            case TType.STRING:
                return STRING;
            default:
                throw new IllegalArgumentException(String.format("unknown thrift byte type: %d", tType));
        }
    }

    public static DataItemTypes fromString(String str)
    {
        return DataItemTypes.valueOf(str.toUpperCase());
    }
}
