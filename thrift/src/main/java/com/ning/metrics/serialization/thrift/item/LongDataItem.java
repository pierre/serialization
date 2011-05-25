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

package com.ning.metrics.serialization.thrift.item;

import org.apache.thrift.protocol.TType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class LongDataItem implements DataItem
{
    private Long value;

    public LongDataItem()
    {
        value = 0L;
    }

    public LongDataItem(final long value)
    {
        this.value = value;
    }

    @Override
    public Boolean getBoolean()
    {
        return !value.equals(0L);
    }

    @Override
    public Byte getByte()
    {
        return value.byteValue();
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
        return value;
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
    public int compareTo(final Object o)
    {
        return value.compareTo(((DataItem) o).getLong());
    }

    @Override
    public boolean equals(final Object o)
    {
        return o instanceof DataItem && value.equals(((DataItem) o).getLong());
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public void write(final DataOutput out) throws IOException
    {
        out.writeByte(LONG_TYPE);
        out.writeLong(value);
    }

    @Override
    public void readFields(final DataInput in) throws IOException
    {
        value = in.readLong();
    }

    @Override
    public String toString()
    {
        return getString();
    }

    @Override
    public byte getThriftType()
    {
        return TType.I64;
    }
}
