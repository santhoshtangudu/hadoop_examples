package com.hadoop.examples.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by santhosh.tangudu on 18/05/16.
 */
public class JobLauncher {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = new Job(conf, "word count");

        job.setReducerClass(WordCountReducer.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setMapperClass(WordCountMapper.class);


        job.setJarByClass(JobLauncher.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}
