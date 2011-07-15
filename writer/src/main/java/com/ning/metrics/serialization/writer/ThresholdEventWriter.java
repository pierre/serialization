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

package com.ning.metrics.serialization.writer;

import com.mogwee.executors.FailsafeScheduledExecutor;
import com.ning.metrics.serialization.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper around another delegate writer. Ensures to commit on the delegate writer after a specified number of
 * uncommitted writes and/or delay.
 * <p/>
 * This writer passes all events to the underlying, delegate, writer for each write call and keeps stats on the total number
 * of events passed. It will trigger a commit (not a forced commit) on the delegate writer after the specified number of writes
 * is reached and/or if the time elapsed since the last commit is greater than the specified flush period.
 * This writer will never trigger a forceCommit on the delegate writer.
 */
public class ThresholdEventWriter implements EventWriter
{
    private static final Logger log = LoggerFactory.getLogger(ThresholdEventWriter.class);

    private final EventWriter delegate;
    private final AtomicLong maxWriteCount;
    private final ScheduledExecutorService executor;

    private volatile long maxFlushPeriodNanos;

    private long lastFlushNanos;
    private long uncommittedWriteCount = 0;
    private boolean acceptsEvents = false;

    public ThresholdEventWriter(final EventWriter delegate, final long maxUncommittedWriteCount, final long maxFlushPeriodInSeconds)
    {
        this.delegate = delegate;
        this.maxWriteCount = new AtomicLong(maxUncommittedWriteCount);
        setMaxFlushPeriodInSeconds(maxFlushPeriodInSeconds);
        this.lastFlushNanos = getNow();

        this.executor = new FailsafeScheduledExecutor("ThresholdEventWriterCommitter");
        executor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    commitIfNeeded();
                }
                catch (IOException e) {
                    log.warn(String.format("Got exception while trying to commit: %s", e));
                }
            }
        }, maxFlushPeriodInSeconds, maxFlushPeriodInSeconds, TimeUnit.SECONDS);

        acceptsEvents = true;
    }

    /**
     * Write an Event via the delegate writer
     *
     * @param event the Event to write
     * @throws IOException as thrown by the delegate writer
     */
    @Override
    public synchronized void write(final Event event) throws IOException
    {
        if (!acceptsEvents) {
            log.warn("Writer not ready, discarding event: {}", event);
            return;
        }

        delegate.write(event);
        uncommittedWriteCount++;

        commitIfNeeded();
    }

    /**
     * Perform a commit via the delegate writer
     *
     * @throws IOException as thrown by the delegate writer
     */
    @Managed(description = "Commit locally spooled events for flushing")
    @Override
    public synchronized void forceCommit() throws IOException
    {
        log.debug("Performing commit on delegate EventWriter [{}]", delegate.getClass());
        delegate.commit();

        uncommittedWriteCount = 0;
        lastFlushNanos = getNow();
    }

    /**
     * Perform a commit, only if needed, i.e. if and only if one of the two conditions are met:
     * <ul>
     * <li>number of uncommitted writes greater than the specified limit
     * <li>time since last flush greater than the specified limit
     * </ul>
     *
     * @throws IOException as thrown by the delegate writer
     */
    @Override
    public synchronized void commit() throws IOException
    {
        commitIfNeeded();
    }

    /**
     * Rollback via the delegate writer
     *
     * @throws IOException as thrown by the delegate writer
     */
    @Override
    public synchronized void rollback() throws IOException
    {
        delegate.rollback();
    }

    /**
     * Flush events: invoke the event handler which will process all events from the final spool area
     *
     * @throws IOException as thrown by the event handler
     */
    @Override
    public synchronized void flush() throws IOException
    {
        delegate.flush();
    }

    @Override
    public synchronized void close() throws IOException
    {
        acceptsEvents = false;

        // Stop the flusher
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();

        // Close the underlying writer
        delegate.close();
    }

    /**
     * @return local spool path used by the writer
     */
    @Override
    public String getSpoolPath()
    {
        return delegate.getSpoolPath();
    }

    private synchronized void commitIfNeeded() throws IOException
    {
        if (uncommittedWriteCount > maxWriteCount.get() || (getNow() - maxFlushPeriodNanos > lastFlushNanos)) {
            forceCommit();
        }
    }

    /**
     * Get the current nanoTime. This is a unit testing hook.
     *
     * @return the current nanoTime
     */
    protected long getNow()
    {
        return System.nanoTime();
    }

    @Managed(description = "Set the max number of writes before a commit is performed")
    public void setMaxWriteCount(final long maxWriteCount)
    {
        this.maxWriteCount.set(maxWriteCount);
    }

    @Managed(description = "The max number of writes before a commit is performed")
    public long getMaxWriteCount()
    {
        return maxWriteCount.get();
    }

    @Managed(description = "Set the max number of seconds between commits of local disk spools")
    public void setMaxFlushPeriodInSeconds(final long maxFlushPeriodInSeconds)
    {
        this.maxFlushPeriodNanos = maxFlushPeriodInSeconds * 1000000000;
    }

    @Managed(description = "The max number of seconds between commits of local disk spools")
    public long getMaxFlushPeriodInSeconds()
    {
        return maxFlushPeriodNanos / 1000000000;
    }
}
