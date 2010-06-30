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

public class BooleanDataItem implements DataItem
{
    private Boolean value;

    public BooleanDataItem()
    {
        value = true;
    }

    public BooleanDataItem(boolean value)
    {
        this.value = value;
    }

    @Override
    public Boolean getBoolean()
    {
        return value;
    }

    @Override
    public Byte getByte()
    {
        return (byte) (value ? 1 : 0);
    }

    @Override
    public Integer getInteger()
    {
        return getByte().intValue();
    }

    @Override
    public Short getShort()
    {
        return getByte().shortValue();
    }

    @Override
    public Long getLong()
    {
        return getByte().longValue();
    }

    @Override
    public String getString()
    {
        return value ? "1" : "0";
    }

    @Override
    public Double getDouble()
    {
        return getByte().doubleValue();
    }

    @Override
    public Comparable getComparable()
    {
        return value;
    }

    @Override
    public int compareTo(Object o)
    {
        return value.compareTo(((DataItem) o).getBoolean());
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof DataItem && value.equals(((DataItem) o).getBoolean());
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeByte(BOOLEAN_TYPE);
        out.writeBoolean(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        value = in.readBoolean();
    }

    @Override
    public String toString()
    {
        return getString();
    }

    @Override
    public byte getThriftType()
    {
        return TType.BOOL;
    }
}