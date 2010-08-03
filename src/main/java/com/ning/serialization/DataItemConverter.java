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

public class DataItemConverter
{
    public static DataItem convert(DataItem dataItem, DataItemTypes type)
    {
        switch (type) {
            case BOOLEAN:
                return new BooleanDataItem(dataItem.getBoolean());
            case BYTE:
                return new ByteDataItem(dataItem.getByte());
            case SHORT:
                return new ShortDataItem(dataItem.getShort());
            case INTEGER:
                return new IntegerDataItem(dataItem.getInteger());
            case LONG:
                return new LongDataItem(dataItem.getLong());
            case DOUBLE:
                return new DoubleDataItem(dataItem.getDouble());
            case STRING:
                return new StringDataItem(dataItem.getString());
        }

        throw new IllegalArgumentException(String.format("for some reason, %s didn't match a case in the switch statement", type));
    }
}