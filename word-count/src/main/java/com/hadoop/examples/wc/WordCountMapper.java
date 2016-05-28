package com.hadoop.examples.wc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by santhosh.tangudu on 18/05/16.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while( itr.hasMoreTokens()){
            String token = itr.nextToken();
            context.write(new Text(token), new LongWritable(1));
        }

    }
}
