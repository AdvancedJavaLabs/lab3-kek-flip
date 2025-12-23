package org.itmo.aggr;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AggrWritable implements Writable {
    private final DoubleWritable revenueWritable = new DoubleWritable();
    private final LongWritable quantityWritable = new LongWritable();

    public AggrWritable() {
    }

    public void setRevenue(double revenue) {
        revenueWritable.set(revenue);
    }

    public void setQuantity(long quantity) {
        quantityWritable.set(quantity);
    }

    public double getRevenue() {
        return revenueWritable.get();
    }

    public long getQuantity() {
        return quantityWritable.get();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        revenueWritable.write(dataOutput);
        quantityWritable.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        revenueWritable.readFields(dataInput);
        quantityWritable.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "#%.2f#%d".formatted(revenueWritable.get(), quantityWritable.get());
    }
}
