package com.ning.metrics.serialization.writer;

import com.ning.metrics.serialization.event.Event;

import java.io.File;

public interface CallbackHandler
{
    public void onError(Throwable t, File file);

    public void onSuccess(File obj);
}
