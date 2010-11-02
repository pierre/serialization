package com.ning.metrics.serialization.event;

import org.joda.time.DateTime;
import org.joda.time.MutableInterval;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collection;

public class TestGranularityPathMapper
{
    private static final String PREFIX = "fuu";

    @Test(groups = "fast")
    public void testMinutly()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.MINUTE);

        Assert.assertEquals(prefix.getPrefix(), PREFIX);
        Assert.assertEquals(prefix.getRootPath(), PREFIX);
        Assert.assertEquals(prefix.getGranularity(), Granularity.MINUTE);

        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01")), String.format("%s/2010/10/01/00/00", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01T07:12:11")), String.format("%s/2010/10/01/07/12", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-25T07:59:59")), String.format("%s/2010/10/25/07/59", PREFIX));
    }

    @Test(groups = "fast")
    public void testHourly()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.HOURLY);

        Assert.assertEquals(prefix.getPrefix(), PREFIX);
        Assert.assertEquals(prefix.getRootPath(), PREFIX);
        Assert.assertEquals(prefix.getGranularity(), Granularity.HOURLY);

        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01")), String.format("%s/2010/10/01/00", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01T07:12:11")), String.format("%s/2010/10/01/07", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-25T07:59:59")), String.format("%s/2010/10/25/07", PREFIX));
    }

    @Test(groups = "fast")
    public void testDaily()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.DAILY);

        Assert.assertEquals(prefix.getPrefix(), PREFIX);
        Assert.assertEquals(prefix.getRootPath(), PREFIX);
        Assert.assertEquals(prefix.getGranularity(), Granularity.DAILY);

        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01")), String.format("%s/2010/10/01", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01T07:12:11")), String.format("%s/2010/10/01", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-25T07:59:59")), String.format("%s/2010/10/25", PREFIX));
    }

    @Test(groups = "fast")
    public void testWeekly()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.WEEKLY);

        Assert.assertEquals(prefix.getPrefix(), PREFIX);
        Assert.assertEquals(prefix.getRootPath(), PREFIX);
        Assert.assertEquals(prefix.getGranularity(), Granularity.WEEKLY);

        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01")), String.format("%s/2010/10/01", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01T07:12:11")), String.format("%s/2010/10/01", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-25T07:59:59")), String.format("%s/2010/10/25", PREFIX));
    }

    @Test(groups = "fast")
    public void testMonthly()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.MONTHLY);

        Assert.assertEquals(prefix.getPrefix(), PREFIX);
        Assert.assertEquals(prefix.getRootPath(), PREFIX);
        Assert.assertEquals(prefix.getGranularity(), Granularity.MONTHLY);

        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01")), String.format("%s/2010/10", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01T07:12:11")), String.format("%s/2010/10", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-25T07:59:59")), String.format("%s/2010/10", PREFIX));
    }

    @Test(groups = "fast")
    public void testYearly()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.YEARLY);

        Assert.assertEquals(prefix.getPrefix(), PREFIX);
        Assert.assertEquals(prefix.getRootPath(), PREFIX);
        Assert.assertEquals(prefix.getGranularity(), Granularity.YEARLY);

        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01")), String.format("%s/2010", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-01T07:12:11")), String.format("%s/2010", PREFIX));
        Assert.assertEquals(prefix.getPathForDateTime(new DateTime("2010-10-25T07:59:59")), String.format("%s/2010", PREFIX));
    }

    @Test(groups = "fast")
    public void testInterval()
    {
        GranularityPathMapper prefix = new GranularityPathMapper(PREFIX, Granularity.DAILY);

        Collection<String> paths = prefix.getPathsForInterval(new MutableInterval(new DateTime("2010-10-01").getMillis(), new DateTime("2010-10-04").getMillis()));
        Object[] pathsArray = paths.toArray();

        Assert.assertEquals(pathsArray.length, 3);
        Assert.assertEquals((String) pathsArray[0], String.format("%s/2010/10/01", PREFIX));
        Assert.assertEquals((String) pathsArray[1], String.format("%s/2010/10/02", PREFIX));
        Assert.assertEquals((String) pathsArray[2], String.format("%s/2010/10/03", PREFIX));
    }
}
