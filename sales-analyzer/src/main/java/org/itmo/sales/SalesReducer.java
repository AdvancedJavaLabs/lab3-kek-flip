package org.itmo.sales;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, SalesWritable, Text, SalesWritable> {
    SalesWritable salesWritable = new SalesWritable();

    @Override
    protected void reduce(Text key, Iterable<SalesWritable> values, Context context) throws IOException, InterruptedException {
        double ttlRevenue = 0.0;
        long ttlQuantity = 0L;

        for (var value : values) {
            ttlRevenue += value.getRevenue();
            ttlQuantity += value.getQuantity();
        }

        salesWritable.setRevenue(ttlRevenue);
        salesWritable.setQuantity(ttlQuantity);

        context.write(key, salesWritable);
    }
}
