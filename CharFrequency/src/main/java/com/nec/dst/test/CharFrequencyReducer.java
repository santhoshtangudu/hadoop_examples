package com.nec.dst.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by santhosh.tangudu on 20/05/16.
 */
public class CharFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        Iterator<LongWritable> iterator = values.iterator();
        while(iterator.hasNext()) {
            long v = iterator.next().get();
            sum += v;
        }
        context.write(key, new LongWritable(sum));
    }
}
