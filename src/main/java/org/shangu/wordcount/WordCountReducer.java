package org.shangu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop_spark_java
 * @Package org.shangu.wordcount
 * @Description: TODO
 * @date Date : 2019-05-02 15:39
 */

public class WordCountReducer extends Reducer<Text, IntWritable, Text,IntWritable> {

    private IntWritable value = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        value.set(sum);
        context.write(key, value);
    }
}
