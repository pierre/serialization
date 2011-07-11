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

package com.ning.metrics.serialization.event;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Event representation of a single Smile event. This class is useful to send Json trees
 * to the collector via the eventtracker library.
 * Under the cover though, we use SmileBucketEvent on the wire (when sending to the collector) and in
 * Hadoop sequencefiles (to leverage Smile back-references).
 *
 * @see com.ning.metrics.serialization.event.SmileBucketEvent
 */
public class SmileEnvelopeEvent implements Event
{
    // UTF-8 won't work!
    public static final Charset CHARSET = Charset.forName("ISO-8859-1");

    public static final Charset NAME_CHARSET = Charset.forName("UTF-8");

    protected static final SmileFactory smileFactory = new SmileFactory();

    static {
        // yes, full 'compression' by checking for repeating names, short string values:
        smileFactory.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        smileFactory.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        // and for now let's not mandate header for input
        smileFactory.configure(SmileParser.Feature.REQUIRE_HEADER, false);
    }

    private static final ObjectMapper smileObjectMapper = new ObjectMapper(smileFactory);

    public static final String SMILE_EVENT_DATETIME_TOKEN_NAME = "eventDate";
    public static final String SMILE_EVENT_GRANULARITY_TOKEN_NAME = "eventGranularity";

    protected DateTime eventDateTime = null;
    protected String eventName;
    protected Granularity granularity = null;
    protected JsonNode root;

    // Should this Event be serialized as Smile or Json?
    private boolean isPlainJson = false;

    private volatile byte[] serializedEvent;

    @Deprecated
    public SmileEnvelopeEvent()
    {
    }

    /**
     * Given a map ("json-like"), create an event with hourly granularity
     *
     * @param eventName     name of the event
     * @param eventDateTime event timestamp
     * @param map           event data
     * @throws IOException generic serialization exception
     */
    public SmileEnvelopeEvent(final String eventName, final DateTime eventDateTime, final Map<String, Object> map) throws IOException
    {
        this.eventName = eventName;
        this.eventDateTime = eventDateTime;
        this.granularity = Granularity.HOURLY;

        ObjectNode root = getObjectMapper().createObjectNode();

        root.put(SMILE_EVENT_DATETIME_TOKEN_NAME, eventDateTime.getMillis());
        root.put(SMILE_EVENT_GRANULARITY_TOKEN_NAME, granularity.toString());

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            addToTree(root, entry.getKey(), entry.getValue());
        }
        this.root = root;
    }

    public SmileEnvelopeEvent(final String eventName, final JsonNode node)
    {
        this(eventName, null, node);
    }

    public SmileEnvelopeEvent(final String eventName, final Granularity granularity, final JsonNode node)
    {
        this.eventName = eventName;
        this.root = node;
        this.granularity = granularity;

        setEventPropertiesFromNode(node);
    }

    public SmileEnvelopeEvent(final String eventName, final byte[] inputBytes, final DateTime eventDateTime, final Granularity granularity) throws IOException
    {
        this.eventName = eventName;
        this.serializedEvent = inputBytes;
        this.eventDateTime = eventDateTime;
        this.granularity = granularity;
        this.root = parseAsTree(inputBytes);
    }

    // this constructor needs a node arg generated via writeToJsonGenerator()
    // can throw RuntimeExceptions very easily, because any JsonNode.get() call return null
    public SmileEnvelopeEvent(final JsonNode node) throws IOException
    {
        eventName = node.path("eventName").getValueAsText();
        root = node.get("payload");
        if ((root == null || root.size() == 0) || (eventName == null || eventName.isEmpty())) {
            throw new IOException("Cannot construct a SmileEnvelopeEvent from just a JsonNode unless JsonNode has eventName and payload properties.");
        }
        setEventPropertiesFromNode(root);
    }

    @Override
    public DateTime getEventDateTime()
    {
        return eventDateTime;
    }

    @Override
    public String getName()
    {
        return eventName;
    }

    @Override
    public Granularity getGranularity()
    {
        return granularity;
    }

    @Override
    public String getVersion()
    {
        // TODO Not sure how to version these schemata. Need more thinking here.
        return "1";
    }

    @Override
    public String getOutputDir(final String prefix)
    {
        final GranularityPathMapper pathMapper = new GranularityPathMapper(String.format("%s/%s", prefix, eventName), granularity);

        return pathMapper.getPathForDateTime(getEventDateTime());
    }

    /**
     * @return a JsonNode representation of a SMILE event (json)
     */
    @Override
    public Object getData()
    {
        return root;
    }

    public ObjectMapper getObjectMapper()
    {
        return smileObjectMapper;
    }

    @Override
    public byte[] getSerializedEvent()
    {
        if (serializedEvent == null) {
            // can we not avoid serializing it if we already have bytes?
            try {
                serializedEvent = getObjectMapper().writeValueAsBytes(root);
            }
            catch (IOException e) { // would rather this was thrown, but signature won't allow it:
                return null;
            }
        }
        return serializedEvent;
    }

    /**
     * The object implements the writeExternal method to save its contents
     * by calling the methods of DataOutput for its primitive values or
     * calling the writeObject method of ObjectOutput for objects, strings,
     * and arrays.
     *
     * @param out the stream to write the object to
     * @throws java.io.IOException Includes any I/O exceptions that may occur
     * @serialData Overriding methods should use this tag to describe
     * the data layout of this Externalizable object.
     * List the sequence of element types and, if possible,
     * relate the element to a public/protected field and/or
     * method of this Externalizable class.
     */
    @Override
    public void writeExternal(final ObjectOutput out) throws IOException
    {
        // Name of the event
        final byte[] eventNameBytes = eventName.getBytes(NAME_CHARSET);
        out.writeInt(eventNameBytes.length);
        out.write(eventNameBytes);

        final byte[] payloadBytes = getSerializedEvent();

        // Size of Smile payload. Needed for deserialization, see below
        out.writeInt(payloadBytes.length);

        out.write(payloadBytes);
    }

    /**
     * The object implements the readExternal method to restore its
     * contents by calling the methods of DataInput for primitive
     * types and readObject for objects, strings and arrays.  The
     * readExternal method must read the values in the same sequence
     * and with the same types as were written by writeExternal.
     *
     * @param in the stream to read data from in order to restore the object
     * @throws java.io.IOException if I/O errors occur
     */
    @Override
    public void readExternal(final ObjectInput in) throws IOException
    {
        // Name of the event first
        final int smileEventNameBytesSize = in.readInt();
        final byte[] eventNameBytes = new byte[smileEventNameBytesSize];
        in.readFully(eventNameBytes);
        eventName = new String(eventNameBytes, NAME_CHARSET);

        // Then payload
        final int smilePayloadSize = in.readInt();
        final byte[] smilePayload = new byte[smilePayloadSize];
        in.readFully(smilePayload);

        root = parseAsTree(smilePayload);

        setEventPropertiesFromNode(root);
    }

    // By using the same JsonGenerator for writing multiple events, we can do streaming smile compression
    // So we can compress multiple events into a single smile stream w/ back-references and everything WITHOUT
    // having to know all the events ahead of time.
    public void writeToJsonGenerator(final JsonGenerator gen) throws IOException
    {
        // writes '{eventName:<name>,payload:{<data>}}' --it's kind of silly but ultimately inconsequential to nest them like this.
        gen.writeStartObject();
        gen.writeStringField("eventName", eventName);
        gen.writeFieldName("payload");
        // TODO for some reason this works regardless of whether JsonObjectMapper or SmileObjectMapper is used
        // which is super convenient, but also mysterious
        getObjectMapper().writeTree(gen, root);
        gen.writeEndObject();
    }

    /**
     * Used as a metadata when the Event is passed around
     *
     * @return true if the underlying payload is/should be plain Json and not Smile
     */
    public boolean isPlainJson()
    {
        return isPlainJson;
    }

    public void setPlainJson(final boolean plainJson)
    {
        isPlainJson = plainJson;
    }

    private void setEventPropertiesFromNode(final JsonNode node)
    {
        eventDateTime = getEventDateTimeFromJson(node);

        if (granularity == null) {
            granularity = getGranularityFromJson(node);
        }
    }

    public static DateTime getEventDateTimeFromJson(final JsonNode node)
    {
        final JsonNode eventDateTimeNode = node.path(SMILE_EVENT_DATETIME_TOKEN_NAME);

        DateTime nodeDateTime = new DateTime();
        if (!eventDateTimeNode.isMissingNode()) {
            nodeDateTime = new DateTime(eventDateTimeNode.getLongValue());
        }

        return nodeDateTime;
    }

    public static Granularity getGranularityFromJson(final JsonNode node)
    {
        final JsonNode granularityNode = node.path(SMILE_EVENT_GRANULARITY_TOKEN_NAME);

        Granularity nodeGranularity = Granularity.HOURLY;
        if (!granularityNode.isMissingNode()) {
            try {
                nodeGranularity = Granularity.valueOf(granularityNode.getValueAsText());
            }
            catch (IllegalArgumentException e) {
                nodeGranularity = null;
            }
        }

        return nodeGranularity;
    }

    private JsonNode parseAsTree(final byte[] smilePayload) throws IOException
    {
        return getObjectMapper().readTree(new ByteArrayInputStream(smilePayload));
    }

    @Override
    public String toString()
    {
        return root.toString();
    }

    private static void addToTree(ObjectNode root, String name, Object value)
    {
        /* could wrap everything as POJONode, but that's bit inefficient;
         * so let's handle some known cases.
         * (in reality, I doubt there could ever be non-scalars, FWIW, since
         * downstream systems expect simple key/value data)
         */
        if (value instanceof String) {
            root.put(name, (String) value);
            return;
        }
        if (value instanceof Number) {
            Number num = (Number) value;
            if (value instanceof Integer) {
                root.put(name, num.intValue());
            }
            else if (value instanceof Long) {
                root.put(name, num.longValue());
            }
            else if (value instanceof Double) {
                root.put(name, num.doubleValue());
            }
            else {
                root.putPOJO(name, num);
            }
        }
        else if (value == Boolean.TRUE) {
            root.put(name, true);
        }
        else if (value == Boolean.FALSE) {
            root.put(name, false);
        }
        else { // most likely Date
            root.putPOJO(name, value);
        }
    }
}
