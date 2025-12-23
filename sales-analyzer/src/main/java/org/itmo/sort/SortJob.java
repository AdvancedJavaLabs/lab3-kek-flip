package org.itmo.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortJob {
    public static int run(Configuration conf, String outDir, String... inPaths) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Sort job");

        job.setJarByClass(SortJob.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(SortedSalesWritable.class);

        job.setSortComparatorClass(DoubleWritableDescComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(SortedSalesWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outDir));
        for (String path : inPaths) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
