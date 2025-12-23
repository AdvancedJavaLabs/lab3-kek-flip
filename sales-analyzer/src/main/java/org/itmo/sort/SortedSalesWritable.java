package org.itmo.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortedSalesWritable implements Writable {
    private final Text categoryWritable = new Text();
    private final DoubleWritable revenueWritable = new DoubleWritable();
    private final LongWritable quantityWritable = new LongWritable();

    public SortedSalesWritable() {
    }

    public void setCategory(String category) {
        categoryWritable.set(category);
    }

    public void setRevenue(double revenue) {
        revenueWritable.set(revenue);
    }

    public void setQuantity(long quantity) {
        quantityWritable.set(quantity);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        categoryWritable.write(dataOutput);
        revenueWritable.write(dataOutput);
        quantityWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        categoryWritable.readFields(dataInput);
        revenueWritable.readFields(dataInput);
        quantityWritable.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "%s,%.2f,%d".formatted(categoryWritable.toString(), revenueWritable.get(), quantityWritable.get());
    }
}
