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

public class ByteDataItem implements DataItem
{
    private Byte value;

    public ByteDataItem()
    {
        value = 0;
    }

    public ByteDataItem(byte value)
    {
        this.value = value;
    }

    @Override
    public Boolean getBoolean()
    {
        return !value.equals((byte) 0);
    }

    @Override
    public Byte getByte()
    {
        return value;
    }

    @Override
    public Integer getInteger()
    {
        return value.intValue();
    }

    @Override
    public Short getShort()
    {
        return value.shortValue();
    }

    @Override
    public Long getLong()
    {
        return value.longValue();
    }

    @Override
    public String getString()
    {
        return String.valueOf(value);
    }

    @Override
    public Double getDouble()
    {
        return value.doubleValue();
    }

    @Override
    public Comparable getComparable()
    {
        return value;
    }

    @Override
    public int compareTo(Object o)
    {
        return value.compareTo(((DataItem) o).getByte());
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof DataItem && value.equals(((DataItem) o).getByte());
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeByte(BYTE_TYPE);
        out.writeByte(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        value = in.readByte();
    }

    @Override
    public String toString()
    {
        return getString();
    }

    @Override
    public byte getThriftType()
    {
        return TType.BYTE;
    }
}