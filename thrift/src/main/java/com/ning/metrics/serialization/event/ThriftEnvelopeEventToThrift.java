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

package com.ning.metrics.serialization.event;

import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftField;

import java.io.Serializable;
import java.lang.reflect.Field;

public class ThriftEnvelopeEventToThrift
{
    public static <T extends Serializable> T extractThrift(Class<T> clazz, ThriftEnvelopeEvent envelopeEvent)
    {
        ThriftEnvelope envelope = (ThriftEnvelope) envelopeEvent.getData();

        try {
            T thriftObject = clazz.newInstance();

            Field[] fields = clazz.getFields();
            for (ThriftField tField : envelope.getPayload()) {
                if (tField.getId() >= fields.length) {
                    continue; // ignore field
                }

                // Thrift fields start at 1, not 0
                Field field = fields[tField.getId() - 1];
                Class type = field.getType();
                if (type.isAssignableFrom(Boolean.class)) {
                    field.setBoolean(thriftObject, tField.getDataItem().getBoolean());
                }
                else if (type.isAssignableFrom(boolean.class)) {
                    field.setBoolean(thriftObject, tField.getDataItem().getBoolean());
                }
                else if (type.isAssignableFrom(Byte.class)) {
                    field.setByte(thriftObject, tField.getDataItem().getByte());
                }
                else if (type.isAssignableFrom(byte.class)) {
                    field.setByte(thriftObject, tField.getDataItem().getByte());
                }
                else if (type.isAssignableFrom(Short.class)) {
                    field.setShort(thriftObject, tField.getDataItem().getShort());
                }
                else if (type.isAssignableFrom(short.class)) {
                    field.setShort(thriftObject, tField.getDataItem().getShort());
                }
                else if (type.isAssignableFrom(Integer.class)) {
                    field.setInt(thriftObject, tField.getDataItem().getInteger());
                }
                else if (type.isAssignableFrom(int.class)) {
                    field.setInt(thriftObject, tField.getDataItem().getInteger());
                }
                else if (type.isAssignableFrom(Long.class)) {
                    field.setLong(thriftObject, tField.getDataItem().getLong());
                }
                else if (type.isAssignableFrom(long.class)) {
                    field.setLong(thriftObject, tField.getDataItem().getLong());
                }
                else if (type.isAssignableFrom(Double.class)) {
                    field.setDouble(thriftObject, tField.getDataItem().getDouble());
                }
                else if (type.isAssignableFrom(double.class)) {
                    field.setDouble(thriftObject, tField.getDataItem().getDouble());
                }
                else if (type.isAssignableFrom(String.class)) {
                    field.set(thriftObject, tField.getDataItem().getString());
                }
            }

            return thriftObject;
        }
        catch (InstantiationException e) {
            return null;
        }
        catch (IllegalAccessException e) {
            return null;
        }
    }
}
