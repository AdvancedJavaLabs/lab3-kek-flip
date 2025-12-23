package org.itmo;

import org.apache.hadoop.conf.Configuration;
import org.itmo.aggr.AggrJob;
import org.itmo.sort.SortJob;

import java.io.IOException;
import java.util.Arrays;

public class MainJob {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        String splitMinSize = args[1];
        conf.set("mapreduce.input.fileinputformat.split.minsize", splitMinSize);

        Integer numReduceTasks = Integer.parseInt(args[0]);
        String mapredOut = "mapred_out";

        int salesJobResult = AggrJob.run(conf, numReduceTasks, mapredOut, Arrays.copyOfRange(args, 3, args.length));
        if (salesJobResult != 0) {
            System.exit(salesJobResult);
        }

        String sortOut = args[2];
        int sortJobResult = SortJob.run(conf, sortOut, mapredOut);
        System.exit(sortJobResult);
    }
}
