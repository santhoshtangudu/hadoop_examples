package com.hadoop.examples.mm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by santhosh.tangudu on 18/05/16.
 */
public class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long[] a = new long[5];
        long[] b = new long[5];
        // b, 2, 0, 30
        for (Text value : values) {
            String cell[] = value.toString().split(",");
            if (cell[0].contains("a")) // take rows here
            {
                int col = Integer.parseInt(cell[2].trim());
                a[col] = Integer.parseInt(cell[3].trim());
            }
            else if (cell[0].contains("b")) // take col here
            {
                int row = Integer.parseInt(cell[1].trim());
                b[row] = Integer.parseInt(cell[3].trim());
            }
        }  Intellij
        int total = 0;
        for (int i = 0; i < 5; i++) {
            long val = a[i] * b[i];
            total += val;
        }
        context.write(key, new LongWritable(total));
    }
}
