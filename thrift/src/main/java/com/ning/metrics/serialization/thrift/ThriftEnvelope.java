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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class ThriftEnvelope
{
    private String typeName;
    private String name;
    private final List<ThriftField> payload = new ArrayList<ThriftField>();

    //for use with hadoop
    @Deprecated
    public ThriftEnvelope()
    {
    }

    public ThriftEnvelope(String typeName, String name)
    {
        this.typeName = typeName;
        this.name = name;
    }

    public ThriftEnvelope(String typeName)
    {
        this(typeName, typeName);
    }

    public ThriftEnvelope(String typeName, String name, List<ThriftField> list)
    {
        this.typeName = typeName;
        this.name = name;
        payload.addAll(list);
    }

    public ThriftEnvelope(String typeName, List<ThriftField> list)
    {
        this.typeName = typeName;
        this.name = typeName;
        payload.addAll(list);
    }

    public String getTypeName()
    {
        return typeName;
    }

    public String getName()
    {
        return name;
    }

    public String getVersion()
    {
        StringBuilder sb = new StringBuilder(payload.size() * 2);
        Set<Short> idList = new TreeSet<Short>();

        for (ThriftField field : payload) {
            idList.add(field.getId());
        }

        for (Short id : idList) {
            if (sb.length() > 0) {
                sb.append(".");
            }
            sb.append(id);
        }

        return sb.toString();
    }

    // hack method to allow hadoop to re-use this object
    public void replaceWith(ThriftEnvelope thriftEnvelope)
    {
        this.typeName = thriftEnvelope.typeName;
        this.name = thriftEnvelope.name;
        this.payload.clear();
        this.payload.addAll(thriftEnvelope.payload);
    }

    public List<ThriftField> getPayload()
    {
        return payload;
    }

    public byte[][] toByteArray()
    {
        byte[][] data = new byte[payload.size()][];

        int index = 0;
        for (ThriftField field : payload) {
            data[index] = field.toByteArray();
            index++;
        }

        return data;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof ThriftEnvelope &&
            typeName.equals(((ThriftEnvelope) o).typeName) &&
            name.equals(((ThriftEnvelope) o).name) &&
            payload.equals(((ThriftEnvelope) o).payload);
    }

    @Override
    public int hashCode()
    {
        int result = typeName != null ? typeName.hashCode() : 0;

        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (payload != null ? payload.hashCode() : 0);

        return result;
    }

    @Override
    public String toString()
    {
        if (typeName.equals(name)) {
            return String.format("%s (%s) : %s", typeName, getVersion(), payload);
        }
        else {
            return String.format("%s [%s] (%s) : %s", typeName, name, getVersion(), payload);
        }
    }
}
