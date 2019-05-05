package org.shangu.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop_spark_java
 * @Package org.shangu.wordcount
 * @Description: TODO
 * @date Date : 2019-05-02 15:23
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text  text = new Text();

    private final static IntWritable value = new IntWritable(1);

    @Override
    public void map (LongWritable key, Text text, Context context ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(text.toString());
        while (itr.hasMoreTokens()) {
            text.set(itr.nextToken());
            context.write(text, value);
        }
    }
}
