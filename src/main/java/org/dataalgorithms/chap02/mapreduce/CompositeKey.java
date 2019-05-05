package org.dataalgorithms.chap02.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * CompositeKey: represents a pair of
 * (String stockSymbol, long timestamp).
 * <p>
 * We do a primary grouping pass on the stockSymbol field to get
 * all of the data of one type together, and then our "secondary sort"
 * during the shuffle phase uses the timestamp long member to sort
 * the timeseries points so that they arrive at the reducer partitioned
 * and in sorted order.
 * <p>
 * 定义一个组合键
 *
 * @author Mahmoud Parsian
 */
public class CompositeKey implements WritableComparable<CompositeKey> {
    // natural key is (stockSymbol)
    // composite key is a pair (stockSymbol, timestamp)
    private String stockSymbol;
    private long timestamp;

    public CompositeKey(String stockSymbol, long timestamp) {
        set(stockSymbol, timestamp);
    }

    public CompositeKey() {
    }

    public void set(String stockSymbol, long timestamp) {
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }

    public String getStockSymbol() {
        return this.stockSymbol;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // 二进制输入流读入， 但是与chapter1 调用的方法不同 不得其解
        // 调用方法根据属性类型决定
        this.stockSymbol = in.readUTF();
        this.timestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 转化成二进制写出
        out.writeUTF(this.stockSymbol);
        out.writeLong(this.timestamp);
    }

    @Override
    public int compareTo(CompositeKey other) {
        if (this.stockSymbol.compareTo(other.stockSymbol) != 0) {
            return this.stockSymbol.compareTo(other.stockSymbol);
        } else if (this.timestamp != other.timestamp) {
            return timestamp < other.timestamp ? -1 : 1;
        } else {
            return 0;
        }

    }

    // 这里不太懂， 外边已经定义一个 为什么建一个类中类
    public static class CompositeKeyComparator extends WritableComparator {
        public CompositeKeyComparator() {
            // 必须调用父类的构造方法不然编译不通过
            // 父类存在非抽象方法
            super(CompositeKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // 再反序列之前即readFields之前、对比两个序列化的二进制数组
            // 调用父类的方法
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        // 使得 WritableComparator 具有 CompositeKey 的属性？？？
        WritableComparator.define(CompositeKey.class, new CompositeKeyComparator());
    }

}
