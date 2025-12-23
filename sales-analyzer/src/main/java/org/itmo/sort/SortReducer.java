package org.itmo.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, SortedSalesWritable, NullWritable, SortedSalesWritable> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<SortedSalesWritable> values, Context context) throws IOException, InterruptedException {
        for (var value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}
