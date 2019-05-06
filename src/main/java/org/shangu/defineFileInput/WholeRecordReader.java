package org.shangu.defineFileInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop_spark_java
 * @Package org.shangu.defineFileInput
 * @Description: 自定义 路径+文件名称为k， 文件字节流为value， 每每个切片调用一次
 * @date Date : 2019-05-05 22:29
 */

public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private FileSplit split;
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();
    private Text key = new Text();
    private Boolean isProgress = true;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // 获取 fileinput 的配置参数
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {
            // 1 定义缓存区
            byte[] contents = new byte[(int) split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;
            try {
                // 2 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);
                // 3 读取数据
                fis = fs.open(path);
                // 4 读取文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);
                // 5 输出文件内容
                value.set(contents, 0, contents.length);
                // 6 获取文件路径及名称
                String name = path.toString();
                // 7 设置输出的key值
                key.set(name);
            } catch (Exception ignore) {
            } finally {
                // 关闭资源
                IOUtils.closeStream(fis);
            }
            isProgress = false;  // 读取到数据标记false
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
