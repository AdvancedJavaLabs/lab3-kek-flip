package org.itmo.sales;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SalesJob {
    public static int run(Configuration conf, Integer numReduceTasks, String outDir, String[] inPaths) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Sales analysis job");

        job.setJarByClass(SalesJob.class);
        job.setNumReduceTasks(numReduceTasks);

        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SalesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SalesWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outDir));
        for (String path : inPaths) {
            FileInputFormat.addInputPath(job, new Path(path));
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
