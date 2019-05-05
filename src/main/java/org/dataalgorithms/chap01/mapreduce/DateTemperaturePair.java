package org.dataalgorithms.chap01.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The DateTemperaturePair class enable us to represent a
 * composite type of (yearMonth, day, temperature). To persist
 * a composite type (actually any data type) in Hadoop, it has
 * to implement the org.apache.hadoop.io.Writable interface.
 * <p>
 * To compare composite types in Hadoop, it has to implement
 * the org.apache.hadoop.io.WritableComparable interface.
 *
 * @author Mahmoud Parsian
 */
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {
    // 如果需要持久化存储定制的数据类型必须继承Writable接口
    // 如果需要比较，还必须继承 WritableComparable接口

    private final Text yearMonth = new Text();
    private final Text day = new Text();
    private final IntWritable temperature = new IntWritable();


    public DateTemperaturePair() {
    }

    public DateTemperaturePair(String yearMonth, String day, int temperature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.temperature.set(temperature);
    }

    public static DateTemperaturePair read(DataInput in) throws IOException {
        DateTemperaturePair pair = new DateTemperaturePair();
        pair.readFields(in);
        return pair;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        day.write(out);
        temperature.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        temperature.readFields(in);
    }

    @Override
    public int compareTo(DateTemperaturePair pair) {
        // 先按照年月排序
        int compareValue = this.yearMonth.compareTo(pair.getYearMonth());
        if (compareValue == 0) {
            // 其次按温度排序
            compareValue = temperature.compareTo(pair.getTemperature());
        }
        //return compareValue; 		// to sort ascending
        // to sort descending
        return -1 * compareValue;
    }

    public Text getYearMonthDay() {
        return new Text(yearMonth.toString() + day.toString());
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public Text getDay() {
        return day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setYearMonth(String yearMonthAsString) {
        yearMonth.set(yearMonthAsString);
    }

    public void setDay(String dayAsString) {
        day.set(dayAsString);
    }

    public void setTemperature(int temp) {
        temperature.set(temp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        // 温度和年月相等判断为相等
        DateTemperaturePair that = (DateTemperaturePair) o;
        if (temperature != null ? !temperature.equals(that.temperature) : that.temperature != null) {
            return false;
        }
        if (yearMonth != null ? !yearMonth.equals(that.yearMonth) : that.yearMonth != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;
        result = 31 * result + (temperature != null ? temperature.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DateTemperaturePair{yearMonth=");
        builder.append(yearMonth);
        builder.append(", day=");
        builder.append(day);
        builder.append(", temperature=");
        builder.append(temperature);
        builder.append("}");
        return builder.toString();
    }
}
