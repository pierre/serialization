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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

/**
 * A Schema is a collection of SchemaFields.
 *
 * @see SchemaField
 */
public class Schema
{
    private static final long serialVersionUID = 1L;

    private final String name;
    private final HashMap<Short, SchemaField> schemaFields = new HashMap<Short, SchemaField>();

    public Schema(final String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public void addSchemaField(final SchemaField schemaField)
    {
        schemaFields.put(schemaField.getId(), schemaField);
    }

    public SchemaField getFieldByPosition(final short id)
    {
        return schemaFields.get(id);
    }

    public SchemaField getFieldByName(final String name)
    {
        for (final SchemaField field : schemaFields.values()) {
            if (field.getName().equals(name)) {
                return field;
            }
        }

        return null;
    }

    /**
     * Get the schema as a collection of fields.
     * We guarantee the ordering by field id.
     *
     * @return the sorted collection of fields
     */
    public ArrayList<SchemaField> getSchema()
    {
        final ArrayList<SchemaField> items = new ArrayList<SchemaField>(schemaFields.values());

        Collections.sort(items, new Comparator<SchemaField>()
        {
            @Override
            public int compare(final SchemaField left, final SchemaField right)
            {
                return left.getId() - right.getId();
            }
        });

        return items;
    }
}
