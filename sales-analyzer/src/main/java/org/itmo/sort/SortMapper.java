package org.itmo.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, SortedSalesWritable> {
    private final DoubleWritable revenueWritable = new DoubleWritable();
    private final SortedSalesWritable sortedSalesWritable = new SortedSalesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null || line.isBlank()) {
            return;
        }

        String[] tokens = line.split("#");
        String category = tokens[0].trim();
        double revenue = Double.parseDouble(tokens[1].trim());
        long quantity = Long.parseLong(tokens[2].trim());

        revenueWritable.set(revenue);
        sortedSalesWritable.setCategory(category);
        sortedSalesWritable.setRevenue(revenue);
        sortedSalesWritable.setQuantity(quantity);

        context.write(revenueWritable, sortedSalesWritable);
    }
}
