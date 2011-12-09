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

package com.ning.metrics.serialization.thrift;

import com.ning.metrics.serialization.thrift.item.DataItem;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

public class ThriftFieldImpl extends ThriftField
{
    private final TField field;
    private final DataItem dataItem;

    public ThriftFieldImpl(final DataItem dataItem, final short id)
    {
        this.dataItem = dataItem;
        this.field = new TField("", dataItem == null ? TType.VOID : dataItem.getThriftType(), id);
    }

    public ThriftFieldImpl(final DataItem dataItem, final TField field)
    {
        this.dataItem = dataItem;
        this.field = field;
    }

    @Override
    public short getId()
    {
        return field.id;
    }

    @Override
    public DataItem getDataItem()
    {
        return dataItem;
    }

    @Override
    public void write(final TProtocol protocol) throws TException
    {
        if (dataItem == null) {
            return;
        }

        protocol.writeFieldBegin(field);

        switch (field.type) {
            case TType.BOOL:
                protocol.writeBool(dataItem.getBoolean());
                break;
            case TType.BYTE:
                protocol.writeByte(dataItem.getByte());
                break;
            case TType.I16:
                protocol.writeI16(dataItem.getShort());
                break;
            case TType.I32:
                protocol.writeI32(dataItem.getInteger());
                break;
            case TType.I64:
                protocol.writeI64(dataItem.getLong());
                break;
            case TType.STRING:
                protocol.writeString(dataItem.getString());
                break;
            case TType.DOUBLE:
                protocol.writeDouble(dataItem.getDouble());
                break;
            default:
                throw new IllegalArgumentException(String.format("unsupported thrift type %s", field.type));
        }
        protocol.writeFieldEnd();
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("ThriftFieldImpl");
        sb.append("{dataItem=").append(dataItem);
        sb.append(", field=").append(field);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ThriftFieldImpl that = (ThriftFieldImpl) o;

        if (dataItem != null ? !dataItem.equals(that.dataItem) : that.dataItem != null) {
            return false;
        }
        if (field != null ? !field.equals(that.field) : that.field != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = field != null ? field.hashCode() : 0;
        result = 31 * result + (dataItem != null ? dataItem.hashCode() : 0);
        return result;
    }
}
