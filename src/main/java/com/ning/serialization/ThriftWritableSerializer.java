package com.ning.serialization;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ThriftWritableSerializer
{
    private DataOutputStream dataOut;

    public void open(OutputStream out)
    {
        if (out instanceof DataOutputStream) {
            dataOut = (DataOutputStream) out;
        }
        else {
            dataOut = new DataOutputStream(out);
        }
    }

    public void serialize(ThriftWritable w) throws IOException
    {
        w.write(dataOut);
    }

    public void close() throws IOException
    {
        dataOut.close();
    }
}
