package com.nec.dst.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by santhosh.tangudu on 20/05/16.
 */
public class JobLauncher {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = new Job(configuration, "Char Frequency");

        job.setReducerClass(CharFrequencyReducer.class);
        job.setCombinerClass(CharFrequencyReducer.class);
        job.setMapperClass(CharFrequencyMapper.class);


        job.setJarByClass(JobLauncher.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0: 1);

    }
}
