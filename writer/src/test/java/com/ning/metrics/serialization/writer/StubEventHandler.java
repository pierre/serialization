package com.ning.metrics.serialization.writer;

import com.ning.metrics.serialization.event.Event;

import java.io.IOException;
import java.io.ObjectInputStream;

public class StubEventHandler implements EventHandler
{
    private final EventWriter eventWriter;

    public StubEventHandler()
    {
        this.eventWriter = new StubEventWriter();
    }

    public StubEventHandler(EventWriter eventWriter)
    {
        this.eventWriter = eventWriter;
    }

    @Override
    public void handle(ObjectInputStream objectInputStream) throws ClassNotFoundException, IOException
    {
        while (objectInputStream.read() != -1) {
            Event event = (Event) objectInputStream.readObject();
            eventWriter.write(event);
        }

        objectInputStream.close();
        eventWriter.forceCommit();
    }

    @Override
    public void rollback() throws IOException
    {
        eventWriter.rollback();
    }
}
