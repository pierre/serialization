package com.ning.metrics.serialization.writer;

import java.io.IOException;
import java.io.ObjectInputStream;

public interface EventHandler
{
    public void handle(ObjectInputStream in) throws ClassNotFoundException, IOException;

    public void rollback() throws IOException;
}
