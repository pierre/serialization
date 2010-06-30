/*
 * Copyright 2010 Ning, Inc.
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

package com.ning.serialization;

import org.apache.hadoop.io.WritableComparable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * <p>
 * A wrapper class for using Thrift with Hadoop Writables.
 * Note that this approach should be preferred to using BytesWritable
 * since it has a (slightly) more compact binary representation and is compatible with
 * <a href="https://issues.apache.org/jira/browse/HADOOP-1986>Hadoop Serializers</a>.
 * </p>
 * <p/>
 * <p>
 * Instances of this class are not thread-safe.
 * </p>
 */
abstract public class ThriftWritable<T extends TBase> implements WritableComparable
{

    static class Transport extends TIOStreamTransport
    {
        public void setInputStream(InputStream in)
        {
            inputStream_ = in;
        }

        public void setOutputStream(OutputStream out)
        {
            outputStream_ = out;
        }
    }

    private T tbase;
    private Transport transport = new Transport();
    private TProtocol protocol = new TBinaryProtocol(transport);

    public ThriftWritable()
    {
    }

    public ThriftWritable(T tbase)
    {
        this.tbase = tbase;
    }

    public T get()
    {
        return tbase;
    }

    public void set(T tbase)
    {
        this.tbase = tbase;
    }

    public void readFields(DataInput in) throws IOException
    {
        // cast to InputStream - ugly, but should work since all
        // DataInput implementations in Hadoop are actually InputStreams
        transport.setInputStream((InputStream) in);
        try {
            tbase.read(protocol);
        }
        catch (TException e) {
            throw new IOException(e.toString());
        }
    }

    public void write(DataOutput out) throws IOException
    {
        // cast to OutputStream - ugly, but should work
        transport.setOutputStream((OutputStream) out);
        try {
            tbase.write(protocol);
        }
        catch (TException e) {
            throw new IOException(e.toString());
        }
    }

    public int compareTo(Object o)
    {
        throw new UnsupportedOperationException("You need to specify a comparator.");
    }

    protected static int stringCompare(String str1, String str2)
    {
        if (str1 == null) {
            if (str2 == null) {
                return 0;
            }
            else {
                return 1;
            }
        }

        return (str1.compareTo(str2));
    }

    protected static boolean stringEquals(String str1, String str2)
    {
        if (str1 == null) {
            return str2 == null;
        }

        return (str1.equals(str2));
    }

    protected static int stringHashCode(String str)
    {
        return (str == null ? 0 : str.hashCode());
    }

    @Override
    public String toString()
    {
        return tbase.toString();
    }

}
