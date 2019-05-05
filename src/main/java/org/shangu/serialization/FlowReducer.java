package org.shangu.serialization;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop_spark_java
 * @Package org.shangu.serialization
 * @Description: TODO
 * @date Date : 2019-05-03 13:06
 */

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean flowBean = new FlowBean(0, 0);

    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Context context
    ) throws IOException, InterruptedException {
        for(FlowBean value: values) {
            flowBean.set(flowBean.getUpFlow() + value.getUpFlow(), flowBean.getDownFlow() + value.getDownFlow());
        }
        context.write(key, flowBean);
    }

}
