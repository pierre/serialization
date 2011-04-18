package com.ning.metrics.serialization.writer;

import com.ning.metrics.serialization.event.Event;

public interface CallbackHandler
{
    public void onError(Throwable t, Event event);

    public void onSuccess(Event event);
}
