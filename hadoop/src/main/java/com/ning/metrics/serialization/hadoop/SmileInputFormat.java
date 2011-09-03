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

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SmileInputFormat extends InputFormat
{
    /**
     * Get the list of input {@link Path}s for the map-reduce job.
     *
     * @param context The job
     * @return the list of input {@link Path}s for the map-reduce job.
     */
    public static Path[] getInputPaths(JobContext context)
    {
        String dirs = context.getConfiguration().get("mapred.input.dir", "");
        String[] list = StringUtils.split(dirs);
        Path[] result = new Path[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }

    /**
     * List input directories.
     *
     * @param job the job to list input paths for
     * @return array of FileStatus objects
     * @throws IOException if zero items.
     */
    protected List<FileStatus> listStatus(JobContext job) throws IOException
    {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }

        // Get tokens for all the required FileSystems..
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

        List<IOException> errors = new ArrayList<IOException>();
        for (Path p : dirs) {
            FileSystem fs = p.getFileSystem(job.getConfiguration());
            final SmilePathFilter filter = new SmilePathFilter();
            FileStatus[] matches = fs.globStatus(p, filter);
            if (matches == null) {
                errors.add(new IOException("Input path does not exist: " + p));
            }
            else if (matches.length == 0) {
                errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
            }
            else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDir()) {
                        Collections.addAll(result, fs.listStatus(globStat.getPath(), filter));
                    }
                    else {
                        result.add(globStat);
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new InvalidInputException(errors);
        }

        return result;
    }


    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException
    {
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        final List<FileStatus> files = listStatus(jobContext);
        for (FileStatus file : files) {
            final Path path = file.getPath();
            final FileSystem fs = path.getFileSystem(jobContext.getConfiguration());
            final BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, file.getLen());
            final List<String> blkHosts = new ArrayList<String>();
            for (final BlockLocation location : blkLocations) {
                blkHosts.addAll(Arrays.asList(location.getHosts()));
            }

            // TODO Split files =)
            final String[] hosts = blkHosts.toArray(new String[0]);
            splits.add(new FileSplit(path, 0, file.getLen(), hosts));
        }

        return splits;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        taskAttemptContext.setStatus("Creating a SmileRecordReader");
        return new SmileRecordReader();
    }

    private class SmilePathFilter implements PathFilter
    {
        @Override
        public boolean accept(Path path)
        {
            return path.getName().endsWith(".smile");
        }
    }
}
