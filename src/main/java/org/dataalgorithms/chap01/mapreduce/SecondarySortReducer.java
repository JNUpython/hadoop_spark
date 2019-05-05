package org.dataalgorithms.chap01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/** 
 * SecondarySortReducer implements the reduce() function for 
 * the secondary sort design pattern.
 *
 * @author Mahmoud Parsian
 *
 */
// public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, Text, Text> {
// 	// reducer <输入键类型，输入值类型，输出键类型，输出值类型>
//
//     @Override
//     protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//     	StringBuilder builder = new StringBuilder();
//     	for (Text value : values) {
//     		builder.append(value.toString());
//     		builder.append(",");
// 		}
//         context.write(key.getYearMonth(), new Text(builder.toString()));
//     }
// }


public class SecondarySortReducer extends Reducer<DateTemperaturePair, Text, DateTemperaturePair, Text> {
	// reducer <输入键类型，输入值类型，输出键类型，输出值类型>

	@Override
	protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Iterable<Text> values: 多个mapper 输出构成
		StringBuilder builder = new StringBuilder();
		for (Text value : values) {
			builder.append(value.toString());
			builder.append(",");
		}
		context.write(key, new Text(builder.toString()));
	}
}