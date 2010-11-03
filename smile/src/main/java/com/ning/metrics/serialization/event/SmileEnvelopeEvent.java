package com.ning.metrics.serialization.event;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.smile.SmileGenerator;
import org.codehaus.jackson.smile.SmileParser;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.Charset;

public class SmileEnvelopeEvent implements Event
{
    private final Charset CHARSET_LATIN1 = Charset.forName("ISO-8859-1");
    
    private final static SmileFactory factory = new SmileFactory();
    static {
        // yes, full 'compression' by checking for repeating names, short string values:
        factory.configure(SmileGenerator.Feature.CHECK_SHARED_NAMES, true);
        factory.configure(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES, true);
        // and for now let's not mandate header for input
        factory.configure(SmileParser.Feature.REQUIRE_HEADER, false);
    }

    private JsonParser parser;

    public static final String SMILE_EVENT_DATETIME_TOKEN_NAME = "eventDate";
    public static final String SMILE_EVENT_GRANULARITY_TOKEN_NAME = "eventGranularity";

    private DateTime eventDateTime = null;
    private String eventName;
    private Granularity granularity = null;
    private byte[] payload;

    @Deprecated
    public SmileEnvelopeEvent()
    {
    }

    /**
     * Create an event from a binary JSON format
     *
     * @param eventName name the event schema
     * @param message   String representation of a SMILE-serialized event
     * @throws IOException if the payload is invalid
     */
    public SmileEnvelopeEvent(String eventName, String message) throws IOException
    {
        this.eventName = eventName;
        this.payload = message.getBytes(CHARSET_LATIN1);
        parser = factory.createJsonParser(payload);
        parseEvent();
    }

    private void parseEvent() throws IOException
    {
        // Go through the stream once
        JsonToken t = parser.nextToken();
        while (t != null && (eventDateTime == null || granularity == null)) {
            if (t == JsonToken.FIELD_NAME) {
                String name = parser.getCurrentName();
                t = parser.nextValue();
                if (SMILE_EVENT_DATETIME_TOKEN_NAME.equals(name)) {
                    if (!t.isNumeric()) {
                        throw new IOException(String.format("Invalid JSON: property '%s' has non-numeric value type [%s]", name, t.asString()));
                    }
                    try {
                        eventDateTime = new DateTime(parser.getLongValue());
                    }
                    catch (NumberFormatException e) {
                        throw new IOException(String.format("Invalid JSON: [%s] is not a timestamp", t.asString()));
                    }
                } else if (SMILE_EVENT_GRANULARITY_TOKEN_NAME.equals(name)) {
                    String text = parser.getText();
                    try {
                        granularity = Granularity.valueOf(parser.getText());
                    }
                    catch (IllegalArgumentException e) {
                        throw new IOException(String.format("Invalid JSON: property '%s', value '%s' is not a valid granularity",
                                name, text));
                    }
                }
            } else {
                t = parser.nextToken();
            }
        }

        if (granularity == null) {
            granularity = Granularity.HOURLY;
        }
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
    public String getOutputDir(String prefix)
    {
        GranularityPathMapper pathMapper = new GranularityPathMapper(String.format("%s/%s", prefix, eventName), granularity);

        return pathMapper.getPathForDateTime(getEventDateTime());
    }

    /**
     * @return byte array (byte[]) that contains Smile event
     */
    @Override
    public Object getData()
    {
        return payload; // This is a byte[] representation of a serialized SMILE event
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
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.write(payload);
    }

    /**
     * The object implements the readExternal method to restore its
     * contents by calling the methods of DataInput for primitive
     * types and readObject for objects, strings and arrays.  The
     * readExternal method must read the values in the same sequence
     * and with the same types as were written by writeExternal.
     *
     * @param in the stream to read data from in order to restore the object
     * @throws java.io.IOException    if I/O errors occur
     * @throws ClassNotFoundException If the class for an object being
     *                                restored cannot be found.
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int numBytes = in.readInt();
        byte[] bytes = new byte[numBytes];

        in.readFully(bytes);

        payload = bytes;
        parser = factory.createJsonParser(bytes);
        parseEvent();
        // One has to set the name manually
    }

    public void setEventName(String eventName)
    {
        this.eventName = eventName;
    }

    @Override
    public String toString()
    {
        return "SmileEnvelopeEvent{" +
            "eventDateTime=" + eventDateTime +
            ", eventName='" + eventName + '\'' +
            ", granularity=" + granularity +
            ", payloadLength=" + payload.length +
            '}';
    }
}