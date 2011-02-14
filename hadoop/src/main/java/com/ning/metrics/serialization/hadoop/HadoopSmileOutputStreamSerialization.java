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

package com.ning.metrics.serialization.hadoop;

import com.ning.metrics.serialization.smile.SmileOutputStream;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Hadoop serializer for Smile.
 * <p/>
 * It's really similar to a SmileOutputStream (i.e. raw Smile), but we have to serialize the event name
 * as well.
 */
public class HadoopSmileOutputStreamSerialization implements Serialization<SmileOutputStream>
{
    private static final Charset CHARSET = Charset.forName("ISO-8859-1");

    @Override
    public boolean accept(Class<?> c)
    {
        return SmileOutputStream.class.isAssignableFrom(c);
    }

    private static class HadoopSmileOutputStreamDeserializer implements Deserializer<SmileOutputStream>
    {
        private InputStream in;

        /**
         * <p>Prepare the deserializer for reading.</p>
         */
        @Override
        public void open(InputStream in) throws IOException
        {
            this.in = in;
        }

        /**
         * <p>
         * Deserialize the next object from the underlying input stream.
         * If the object <code>t</code> is non-null then this deserializer
         * <i>may</i> set its internal state to the next object read from the input
         * stream. Otherwise, if the object <code>t</code> is null a new
         * deserialized object will be created.
         * </p>
         *
         * @return the deserialized object
         */
        @Override
        public SmileOutputStream deserialize(SmileOutputStream stream) throws IOException
        {
            ByteArrayOutputStream outStream = new ByteArrayOutputStream(1024);
            byte[] buffer = new byte[1024];

            int count;
            while ((count = in.read(buffer)) > 0) {
                outStream.write(buffer, 0, count);
            }

            byte[] bytes = outStream.toByteArray();
            if (bytes.length < 4) {
                return null;
            }

            int eventNameSize = byteArrayToInt(bytes);
            byte[] eventNameBytes = new byte[eventNameSize];
            System.arraycopy(bytes, 4, eventNameBytes, 0, eventNameSize);
            String eventName = new String(eventNameBytes, CHARSET);

            byte[] payload = new byte[bytes.length - 4 - eventNameSize];
            System.arraycopy(bytes, 4 + eventNameSize, payload, 0, payload.length);

            // TODO Should the payload be a power of 2?
            SmileOutputStream finalStream = new SmileOutputStream(eventName, payload.length);
            finalStream.write(payload);

            if (stream != null) {
                stream.write(bytes);
            }

            return finalStream;
        }

        /**
         * <p>Close the underlying input stream and clear up any resources.</p>
         */
        @Override
        public void close() throws IOException
        {
            in.close();
        }
    }

    private static class HadoopSmileOutputStreamSerializer implements Serializer<SmileOutputStream>
    {
        private OutputStream out;

        /**
         * <p>Prepare the serializer for writing.</p>
         */
        @Override
        public void open(OutputStream out) throws IOException
        {
            this.out = out;
        }

        /**
         * <p>Serialize <code>t</code> to the underlying output stream.</p>
         */
        @Override
        public void serialize(SmileOutputStream outStream) throws IOException
        {
            byte[] payload = outStream.toByteArray();
            byte[] eventName = outStream.getTypeName().getBytes(CHARSET);
            int eventNameSize = eventName.length;

            byte[] eventNameSizeBytes = {
                (byte) (eventNameSize >>> 24),
                (byte) (eventNameSize >>> 16),
                (byte) (eventNameSize >>> 8),
                (byte) eventNameSize};

            byte[] eventMetadata = concat(eventNameSizeBytes, eventName);

            out.write(concat(eventMetadata, payload));
        }

        /**
         * <p>Close the underlying output stream and clear up any resources.</p>
         */
        @Override
        public void close() throws IOException
        {
            out.close();
        }
    }

    @Override
    public Deserializer<SmileOutputStream> getDeserializer(Class<SmileOutputStream> c)
    {
        return new HadoopSmileOutputStreamDeserializer();
    }

    @Override
    public Serializer<SmileOutputStream> getSerializer(Class<SmileOutputStream> c)
    {
        return new HadoopSmileOutputStreamSerializer();
    }

    public static byte[] concat(byte[] first, byte[] second)
    {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    public static int byteArrayToInt(byte[] b)
    {
        return (b[0] << 24)
            + ((b[1] & 0xFF) << 16)
            + ((b[2] & 0xFF) << 8)
            + (b[3] & 0xFF);
    }
}

