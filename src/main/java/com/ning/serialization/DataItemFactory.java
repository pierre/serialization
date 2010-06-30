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

public class DataItemFactory
{
    public static DataItem create(Boolean value)
    {
        return new BooleanDataItem(value);
    }

    public static DataItem create(Byte value)
    {
        return new ByteDataItem(value);
    }

    public static DataItem create(Short value)
    {
        return new ShortDataItem(value);
    }

    public static DataItem create(Integer value)
    {
        return new IntegerDataItem(value);
    }

    public static DataItem create(Long value)
    {
        return new LongDataItem(value);
    }

    public static DataItem create(Double value)
    {
        return new DoubleDataItem(value);
    }

    public static DataItem create(String value)
    {
        return new StringDataItem(value);
    }
}