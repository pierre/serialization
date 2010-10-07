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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

public abstract class ThriftField
{
    public abstract short getId();

    public abstract DataItem getDataItem();

    public abstract void write(TProtocol protocol) throws TException;

    public byte[] toByteArray()
    {
        switch (getDataItem().getThriftType()) {
            case TType.BOOL:
                return booleanToByteArray(getDataItem().getBoolean());
            case TType.BYTE:
                return numberToByteArray(Byte.toString(getDataItem().getByte()));
            case TType.I16:
                return numberToByteArray(Short.toString(getDataItem().getShort()));
            case TType.I32:
                return numberToByteArray(Integer.toString(getDataItem().getInteger()));
            case TType.I64:
                return numberToByteArray(Long.toString(getDataItem().getLong()));
            case TType.STRING:
                return getDataItem().getString().getBytes();
            default:
                throw new IllegalStateException("Unsupported field type " + getDataItem().getThriftType());
        }
    }

    private static byte[] numberToByteArray(String string)
    {
        byte[] bytes = new byte[string.length()];

        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) string.charAt(i);
        }
        return bytes;
    }

    private static byte[] booleanToByteArray(boolean b)
    {
        return numberToByteArray(b ? "0" : "1");
    }
}