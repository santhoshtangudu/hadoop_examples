package com.nec.dst.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by santhosh.tangudu on 20/05/16.
 */
public class CharFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    public void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString();
        char[] characters = line.toCharArray();
        for (char character : characters) {
            String ch = String.valueOf(character);
            if(ch != null || ch != " ") {
                context.write(new Text(ch), new LongWritable(1));
            }
        }

    }
}
