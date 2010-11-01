package com.ning.metrics.serialization.event;

import org.joda.time.DateTime;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public class ThriftEvent implements Event
{
    private String eventName;
    private DateTime eventDateTime;
    private Granularity granularity;
    private Object thriftObject;

    public <T extends Serializable> ThriftEvent(String eventName, DateTime eventDateTime, T thriftObject)
    {
        this(eventName, eventDateTime, thriftObject, Granularity.HOURLY);
    }

    public <T extends Serializable> ThriftEvent(String eventName, DateTime eventDateTime, T thriftObject, Granularity granularity)
    {
        this.eventName = eventName;
        this.eventDateTime = eventDateTime;
        this.granularity = granularity;
        this.thriftObject = thriftObject;
    }

    /**
     * Public no-arg constructor (Externalizable)
     */
    public ThriftEvent()
    {
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
        throw new RuntimeException("Not implemented for native Thrift objects");
    }

    @Override
    public String getOutputDir(String prefix)
    {
        GranularityPathMapper pathMapper = new GranularityPathMapper(String.format("%s/%s", prefix, eventName), granularity);

        return pathMapper.getPathForDateTime(getEventDateTime());
    }

    /**
     * @return Object representing the data (ThriftEnvelope, ...)
     */
    @Override
    public Object getData()
    {
        return thriftObject;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException
    {
        objectOutput.writeObject(eventName);
        objectOutput.writeObject(eventDateTime);
        objectOutput.writeObject(granularity);
        objectOutput.writeObject(thriftObject);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException
    {
        eventName = (String) objectInput.readObject();
        eventDateTime = (DateTime) objectInput.readObject();
        granularity = (Granularity) objectInput.readObject();
        thriftObject = objectInput.readObject();
    }
}