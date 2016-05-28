package com.hadoop.examples.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by santhosh.tangudu on 18/05/16.
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {


    @Override
    protected void reduce(Text word, Iterable<LongWritable> valueList, Context context)
            throws IOException, InterruptedException {
        int sum=0;
        Iterator<LongWritable> itr= valueList.iterator();

        while (itr.hasNext()){
            long number = itr.next().get();
            sum  += number;
        }

        context.write(word, new LongWritable(sum));
    }
}
