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

package com.ning.metrics.serialization.schema;

abstract class AbstractSchemaField implements SchemaField
{
    private static final long serialVersionUID = 1L;

    private final String name;
    private final SchemaFieldType type;
    private final short id;

    public AbstractSchemaField(String name, SchemaFieldType type, short id)
    {
        this.name = name;
        this.type = type;
        this.id = id;
    }

    /**
     * Return the field position of the described field in the schema.
     *
     * @return the field position
     */
    @Override
    public short getId()
    {
        return id;
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public SchemaFieldType getType()
    {
        return type;
    }

    @Override
    public int hashCode()
    {
        return 37 * (37 * name.hashCode() ^ type.hashCode()) ^ id;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) return true;
        if (obj == null || !(obj instanceof AbstractSchemaField)) return false;
        AbstractSchemaField other = (AbstractSchemaField) obj;
        return (other.type == type) && (other.id == id) && other.name.equals(name);
    }
}
