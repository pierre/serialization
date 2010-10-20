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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringDataItem implements DataItem
{
    private String value;

    public StringDataItem()
    {
        value = "";
    }

    public StringDataItem(String value)
    {
        if (value == null) {
            throw new NullPointerException();
        }
        this.value = value.trim();
    }

    @Override
    public Boolean getBoolean()
    {
        return Boolean.valueOf(value) || "1".equals(value);
    }

    @Override
    public Byte getByte()
    {
        if (value.isEmpty()) {
            return (byte) 0;
        }
        return Byte.valueOf(value);
    }

    @Override
    public Integer getInteger()
    {
        if (value.isEmpty()) {
            return 0;
        }
        return Integer.valueOf(value);
    }

    @Override
    public Short getShort()
    {
        if (value.isEmpty()) {
            return (short) 0;
        }
        return Short.valueOf(value);
    }

    @Override
    public Long getLong()
    {
        if (value.isEmpty()) {
            return (long) 0;
        }
        return Long.valueOf(value);
    }

    @Override
    public String getString()
    {
        return value;
    }

    @Override
    public Double getDouble()
    {
        if (value.isEmpty()) {
            return (double) 0;
        }
        return Double.valueOf(value);
    }

    @Override
    public Comparable getComparable()
    {
        return value;
    }

    @Override
    public int compareTo(Object o)
    {
        return value.compareTo(((StringDataItem) o).value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof StringDataItem && value.equals(((StringDataItem) o).value);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        byte[] bytes = value.getBytes();

        out.writeByte(STRING_TYPE);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        int length = in.readInt();
        byte[] bytes = new byte[length];

        in.readFully(bytes);
        value = new String(bytes);
    }

    @Override
    public String toString()
    {
        return getString();
    }

    @Override
    public byte getThriftType()
    {
        return TType.STRING;
    }
}