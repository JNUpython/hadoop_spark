package org.shangu.defineFileInput;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop_spark_java
 * @Package org.shangu.defineFileInput
 * @Description: TODO
 * @date Date : 2019-05-06 22:25
 */

public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        // 每个文件对应一个 不需要循环
        context.write(key, values.iterator().next());
    }

}

