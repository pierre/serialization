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
    private final String name;
    private HashMap<Short, SchemaField> schemaFields = new HashMap<Short, SchemaField>();

    public Schema(String name)
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }

    public void addSchemaField(SchemaField schemaField)
    {
        schemaFields.put(schemaField.getId(), schemaField);
    }

    public SchemaField getFieldByPosition(short id)
    {
        return schemaFields.get(id);
    }

    public SchemaField getFieldByName(String name)
    {
        for (SchemaField field : schemaFields.values()) {
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
    @SuppressWarnings("unused")
    public ArrayList<SchemaField> getSchema()
    {
        ArrayList<SchemaField> items = new ArrayList<SchemaField>(schemaFields.values());

        Collections.sort(items, new Comparator<SchemaField>()
        {
            @Override
            public int compare(SchemaField left, SchemaField right)
            {
                return Short.valueOf(left.getId()).compareTo(right.getId());
            }
        });

        return items;
    }
}
