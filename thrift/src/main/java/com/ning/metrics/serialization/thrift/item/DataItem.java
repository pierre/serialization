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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public interface DataItem extends Comparable, Serializable
{
    public Boolean getBoolean();

    public Byte getByte();

    public Short getShort();

    public Integer getInteger();

    public Long getLong();

    public String getString();

    public Double getDouble();

    public Comparable getComparable();

    public byte getThriftType();

    public void write(DataOutput out) throws IOException;

    public void readFields(DataInput in) throws IOException;

    public static byte LONG_TYPE = 0;
    public static byte STRING_TYPE = 1;
    public static byte DOUBLE_TYPE = 2;
    public static byte BOOLEAN_TYPE = 3;
    public static byte INTEGER_TYPE = 4;
    public static byte SHORT_TYPE = 5;
    public static byte BYTE_TYPE = 6;
}