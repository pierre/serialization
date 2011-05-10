package com.ning.metrics.serialization.hadoop.pig;

import com.ning.metrics.goodwill.access.GoodwillAccessor;
import com.ning.metrics.goodwill.access.GoodwillSchema;
import com.ning.metrics.goodwill.access.GoodwillSchemaField;
import com.ning.metrics.serialization.hadoop.HadoopThriftEnvelopeSerialization;
import com.ning.metrics.serialization.hadoop.HadoopThriftWritableSerialization;
import com.ning.metrics.serialization.schema.SchemaFieldType;
import com.ning.metrics.serialization.thrift.ThriftEnvelope;
import com.ning.metrics.serialization.thrift.ThriftField;
import com.ning.metrics.serialization.thrift.hadoop.ThriftWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ThriftStorage extends LoadFunc
{
    private static final Logger log = Logger.getLogger(ThriftStorage.class);

    private final TupleFactory factory = TupleFactory.getInstance();
    private final GoodwillSchema schema;

    private Object value;
    private RecordReader reader;
    private PigSplit split;

    public ThriftStorage(String schemaName) throws IOException
    {
        this(schemaName, System.getProperty("goodwill.host", "127.0.0.1"), Integer.getInteger("goodwill.port", 8080));
    }

    public ThriftStorage(String schemaName, String goodwillHost, int goodwillPort) throws IOException
    {
        try {
            GoodwillAccessor goodwillAccessor = new GoodwillAccessor(goodwillHost, goodwillPort);
            schema = goodwillAccessor.getSchema(schemaName).get();
        }
        catch (InterruptedException e) {
            throw new IOException("Interrupted while trying to fetch Thrift schema", e);
        }
        catch (ExecutionException e) {
            throw new IOException("Exception while trying to fetch Thrfit schema", e);
        }
    }

    /**
     * Communicate to the loader the location of the object(s) being loaded.
     * The location string passed to the LoadFunc here is the return value of
     * {@link org.apache.pig.LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}. Implementations
     * should use this method to communicate the location (and any other information)
     * to its underlying InputFormat through the Job object.
     * <p/>
     * This method will be called in the backend multiple times. Implementations
     * should bear in mind that this method is called multiple times and should
     * ensure there are no inconsistent side effects due to the multiple calls.
     *
     * @param location Location as returned by
     *                 {@link org.apache.pig.LoadFunc#relativeToAbsolutePath(String, org.apache.hadoop.fs.Path)}
     * @param job      the {@link org.apache.hadoop.mapreduce.Job} object
     *                 store or retrieve earlier stored information from the {@link org.apache.pig.impl.util.UDFContext}
     * @throws java.io.IOException if the location is not valid.
     */
    @Override
    public void setLocation(String location, Job job) throws IOException
    {
        setIOSerializations(job.getConfiguration());
        FileInputFormat.setInputPaths(job, location);
    }

    /**
     * This will be called during planning on the front end. This is the
     * instance of InputFormat (rather than the class name) because the
     * load function may need to instantiate the InputFormat in order
     * to control how it is constructed.
     *
     * @return the InputFormat associated with this loader.
     * @throws java.io.IOException if there is an exception during InputFormat
     *                             construction
     */
    @Override
    public InputFormat getInputFormat() throws IOException
    {
        return new SequenceFileInputFormat<ThriftWritable, ThriftEnvelope>();
    }

    /**
     * Initializes LoadFunc for reading data.  This will be called during execution
     * before any calls to getNext.  The RecordReader needs to be passed here because
     * it has been instantiated for a particular InputSplit.
     *
     * @param reader {@link org.apache.hadoop.mapreduce.RecordReader} to be used by this instance of the LoadFunc
     * @param split  The input {@link org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit} to process
     * @throws java.io.IOException if there is an exception during initialization
     */
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException
    {
        this.reader = reader;
        this.split = split;
        setIOSerializations(split.getConf());
    }

    private void setIOSerializations(Configuration conf)
    {
        String[] configuredSerializations = conf.getStrings("io.serializations");
        int i = configuredSerializations.length;

        String[] allSerializations = new String[i + 3];
        System.arraycopy(configuredSerializations, 0, allSerializations, 0, i);

        allSerializations[i] = HadoopThriftWritableSerialization.class.getName();
        allSerializations[i + 1] = HadoopThriftEnvelopeSerialization.class.getName();
        allSerializations[i + 2] = "org.apache.hadoop.io.serializer.WritableSerialization";

        conf.setStrings("io.serializations", allSerializations);
    }

    @Override
    public Tuple getNext() throws IOException
    {
        try {
            List<Object> tupleList = new ArrayList<Object>();

            if (reader == null || !reader.nextKeyValue()) {
                return null;
            }

            value = reader.getCurrentValue();

            if (value instanceof ThriftEnvelope) {
                ThriftEnvelope envelope = (ThriftEnvelope) value;
                for (ThriftField thriftField : envelope.getPayload()) {
                    GoodwillSchemaField schemaField = schema.getFieldByPosition(thriftField.getId());

                    if (schemaField == null) {
                        throw new IOException(String.format("got a thrift ID [%d] that is not part of the schema", thriftField.getId()));
                    }

                    tupleList.add(getPigType(schemaField.getType()));
                }

                return factory.newTuple(tupleList);
            }
            else {
                throw new IOException(String.format("Expected ThriftEnvelope, not %s", value.getClass()));
            }
        }
        catch (InterruptedException e) {
            log.warn("Interrupted getting next tuple", e);
        }

        return null;
    }

    private byte getPigType(SchemaFieldType type)
    {
        switch (type) {
            case BOOLEAN:
                return DataType.INTEGER;
            case BYTE:
                return DataType.BYTE;
            case SHORT:
            case INTEGER:
                return DataType.INTEGER;
            case LONG:
                return DataType.LONG;
            case DOUBLE:
                return DataType.DOUBLE;
            case DATE:
            case IP:
            case STRING:
            default:
                return DataType.CHARARRAY;
        }
    }
}
