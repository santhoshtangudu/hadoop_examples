package com.hadoop.examples.mm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by santhosh.tangudu on 18/05/16.
 */
public class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {

    private long lMax = 5;
    private long iMax = 5;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // input format is ["a", 0, 0, 63]
        String[] csv = value.toString().split(",");
        String matrix = csv[0].trim();
        long row = Long.parseLong(csv[1].trim());
        long col = Long.parseLong(csv[2].trim());
        if(matrix.contains("a"))
        {
            for (long i=0; i < lMax; i++)
            {
                String akey = Long.toString(row) + "," + Long.toString(i);
                context.write(new Text(akey), value);
            }
        }
        if(matrix.contains("b"))
        {
            for (long i=0; i < iMax; i++)
            {
                String akey = Long.toString(i) + "," + Long.toString(col);
                context.write(new Text(akey), value);
            }
        }
    }
}
