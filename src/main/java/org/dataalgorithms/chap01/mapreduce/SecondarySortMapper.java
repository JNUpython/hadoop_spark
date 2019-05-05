package org.dataalgorithms.chap01.mapreduce;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/** 
 * SecondarySortMapper implements the map() function for 
 * the secondary sort design pattern.
 *
 * @author Mahmoud Parsian
 *
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, Text> {
    // 抽象类的4个类型，代表mapper的 《输入键，输入值， 输出键， 输出值》

    // value
    private final Text theTemperature = new Text();
    // key
    private final DateTemperaturePair pair = new DateTemperaturePair();

    @Override
    /**
     * @param key is generated by Hadoop (ignored here)
     * @param value has this format: "YYYY,MM,DD,temperature"
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 前两个参数为mapper的输入键与值 而第三个则是保存mapper的输出， 这里第一个参数key其实没有任何实际用处可以理解为文本的行数吧
        String line = value.toString();
        String[] tokens = line.split(",");
        // YYYY = tokens[0]
        // MM = tokens[1]
        // DD = tokens[2]
        // temperature = tokens[3]
        String yearMonth = tokens[0] + tokens[1];
        String day = tokens[2];
        int temperature = Integer.parseInt(tokens[3]);

        pair.setYearMonth(yearMonth);
        pair.setDay(day);
        pair.setTemperature(temperature);
        theTemperature.set(tokens[3]);

        context.write(pair, theTemperature);
    }
}