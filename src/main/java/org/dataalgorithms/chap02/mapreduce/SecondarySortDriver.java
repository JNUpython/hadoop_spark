package org.dataalgorithms.chap02.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.dataalgorithms.util.HadoopUtil;

/**
 * SecondarySortDriver is driver class for submitting secondary sort job to Hadoop.
 *
 * @author Mahmoud Parsian
 */
public class SecondarySortDriver {

    public static void main(String[] args) throws Exception {
        // 这里没有chapter1继承关系直接定义一个main函数，简介很多
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Secondary Sort");
        // JobConf conf = new JobConf();
        // conf.setJobName("Secondary Sort");
        // Job job = new Job(conf);

        // add jars to distributed cache
        HadoopUtil.addJarsToDistributedCache(conf, "/lib/");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: SecondarySortDriver <input> <output>");
            System.exit(1);
        }

        job.setJarByClass(SecondarySortDriver.class);
        job.setJarByClass(SecondarySortMapper.class);
        job.setJarByClass(SecondarySortReducer.class);

        // set mapper and reducer
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(NaturalValue.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // the following 3 setting are needed for "secondary sorting"
        // Partitioner decides which mapper output goes to which reducer 
        // based on mapper output key. In general, different key is in 
        // different group (Iterator at the reducer side). But sometimes, 
        // we want different key in the same group. This is the time for 
        // Output Value Grouping Comparator, which is used to group mapper 
        // output (similar to group by condition in SQL).  The Output Key 
        // Comparator is used during sort stage for the mapper output key.
        job.setPartitionerClass(NaturalKeyPartitioner.class);
        // 这两者的作用区别在哪
        // 对reducer接受的输入 自然key 进行排序
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        // 得前面一步分组结果后 对组内的顺序再次进行排序
        job.setSortComparatorClass(CompositeKeyComparator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);

    }
}
