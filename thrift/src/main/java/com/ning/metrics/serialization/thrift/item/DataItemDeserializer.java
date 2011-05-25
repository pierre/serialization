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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

import java.io.DataInput;
import java.io.IOException;

public class DataItemDeserializer
{
    public DataItem fromThrift(final TProtocol protocol, final TField field) throws TException
    {
        final DataItem dataItem;

        switch (field.type) {
            case TType.BOOL:
                dataItem = new BooleanDataItem(protocol.readBool());
                break;
            case TType.BYTE:
                dataItem = new ByteDataItem(protocol.readByte());
                break;
            case TType.I16:
                dataItem = new ShortDataItem(protocol.readI16());
                break;
            case TType.I32:
                dataItem = new IntegerDataItem(protocol.readI32());
                break;
            case TType.I64:
                dataItem = new LongDataItem(protocol.readI64());
                break;
            case TType.DOUBLE:
                dataItem = new DoubleDataItem(protocol.readDouble());
                break;
            case TType.STRING:
                dataItem = new StringDataItem(protocol.readString()); //TODO: we only allow strings, this won't if data is binary (let presentation layer deal with this)
                break;
            default:
                throw new IllegalArgumentException(String.format("Unknown type %d", field.type));
        }

        return dataItem;
    }

    public DataItem fromHadoop(final DataInput in) throws IOException
    {
        final DataItem dataItem;
        final byte type = in.readByte();

        if (type == DataItem.BOOLEAN_TYPE) {
            dataItem = new BooleanDataItem();
            dataItem.readFields(in);
        }
        else if (type == DataItem.BYTE_TYPE) {
            dataItem = new ByteDataItem();
            dataItem.readFields(in);
        }
        else if (type == DataItem.SHORT_TYPE) {
            dataItem = new ShortDataItem();
            dataItem.readFields(in);
        }
        else if (type == DataItem.INTEGER_TYPE) {
            dataItem = new IntegerDataItem();
            dataItem.readFields(in);
        }
        else if (type == DataItem.LONG_TYPE) {
            dataItem = new LongDataItem();
            dataItem.readFields(in);
        }
        else if (type == DataItem.DOUBLE_TYPE) {
            dataItem = new DoubleDataItem();
            dataItem.readFields(in);
        }
        else if (type == DataItem.STRING_TYPE) {
            dataItem = new StringDataItem();
            dataItem.readFields(in);
        }
        else {
            throw new IOException(String.format("unknown DataItem type: %d", type));
        }

        return dataItem;
    }
}
