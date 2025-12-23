package org.itmo.sales;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesWritable> {
    private static final String HEADER = "transaction_id,product_id,category,price,quantity";
    private static final int CATEGORY_INDEX = 2;
    private static final int PRICE_INDEX = 3;
    private static final int QUANTITY_INDEX = 4;

    Text categoryWritable = new Text();
    SalesWritable salesWritable = new SalesWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String csvEntry = value.toString();
        if (csvEntry == null) {
            return;
        }
        csvEntry = csvEntry.trim();

        if (csvEntry.isBlank() || csvEntry.equals(HEADER)) {
            return;
        }

        String[] tokens = csvEntry.split(",");
        if (tokens.length < 5) {
            return;
        }

        String category = tokens[CATEGORY_INDEX].trim();
        double price = Double.parseDouble(tokens[PRICE_INDEX].trim());
        long quantity = Long.parseLong(tokens[QUANTITY_INDEX].trim());

        categoryWritable.set(category);
        salesWritable.setRevenue(price * quantity);
        salesWritable.setQuantity(quantity);

        context.write(categoryWritable, salesWritable);
    }
}
