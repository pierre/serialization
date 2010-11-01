package com.ning.metrics.serialization.event;

import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class TestThriftEvent
{
    private static final DateTime EVENT_TIMESTAMP = new DateTime();
    private static final String EVENT_NAME = "logging event";

    private TLoggingEvent thriftObject;
    private ThriftEvent thriftEvent;

    @BeforeTest(alwaysRun = true)
    public void setup()
    {
        thriftObject = new TLoggingEvent(EVENT_TIMESTAMP.getMillis(), "level", "message", "loggerName", "locationFileName", "locationMethodName", "locationClassName", null, null, null, null, null, "strack", "10.0.01", "foo.com", "hello");
        thriftEvent = new ThriftEvent(EVENT_NAME, EVENT_TIMESTAMP, thriftObject);
    }

    @Test
    public void testGetEventDateTime() throws Exception
    {
        Assert.assertEquals(thriftEvent.getEventDateTime(), EVENT_TIMESTAMP);
    }

    @Test
    public void testGetName() throws Exception
    {
        Assert.assertEquals(thriftEvent.getName(), EVENT_NAME);
    }

    @Test
    public void testGetOutputDir() throws Exception
    {
        Assert.assertEquals(thriftEvent.getOutputDir("/bar/baz"), String.format("/bar/baz/%s/%04d/%02d/%02d/%02d", EVENT_NAME, EVENT_TIMESTAMP.getYear(), EVENT_TIMESTAMP.getMonthOfYear(), EVENT_TIMESTAMP.getDayOfMonth(), EVENT_TIMESTAMP.getHourOfDay()));
    }

    @Test
    public void testGetData() throws Exception
    {
        Assert.assertEquals(thriftEvent.getData(), thriftObject);
    }

    @Test
    public void testWriteAndReadExternal() throws Exception
    {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(1024);
        ObjectOutput out = new ObjectOutputStream(stream);
        thriftEvent.writeExternal(out);

        ThriftEvent thriftEvent2 = new ThriftEvent();
        thriftEvent2.readExternal(new ObjectInputStream(new ByteArrayInputStream(stream.toByteArray())));

        Assert.assertEquals(thriftEvent2.getEventDateTime(), thriftEvent.getEventDateTime());
        Assert.assertEquals(thriftEvent2.getName(), thriftEvent.getName());
        Assert.assertEquals(thriftEvent2.getOutputDir("bar/baz"), thriftEvent.getOutputDir("bar/baz"));
        Assert.assertEquals(thriftEvent2.getData(), thriftEvent.getData());
    }
}
