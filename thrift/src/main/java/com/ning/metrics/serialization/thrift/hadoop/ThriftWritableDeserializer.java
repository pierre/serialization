package com.ning.metrics.serialization.thrift.hadoop;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ThriftWritableDeserializer
{
    private DataInputStream dataIn;

    public void open(InputStream in)
    {
        if (in instanceof DataInputStream) {
            dataIn = (DataInputStream) in;
        }
        else {
            dataIn = new DataInputStream(in);
        }
    }

    public ThriftWritable deserialize(ThriftWritable writable) throws IOException
    {
        writable.readFields(dataIn);
        return writable;
    }

    public void close() throws IOException
    {
        dataIn.close();
    }
}
