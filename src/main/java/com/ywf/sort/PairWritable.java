package com.ywf.sort;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * ClassName PairWritable
 * 功能: 二次排序
 * Author yangweifeng
 * Date 2019-08-02 16:44
 * Version 1.0
 **/
public class PairWritable implements WritableComparable<PairWritable> {
    private  String first;
    private  int second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public PairWritable(String first, int second) {
        set(first,second);
    }

    public PairWritable() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PairWritable that = (PairWritable) o;
        return second == that.second && Objects.equals(first, that.first);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    public  void set(String first, int second){
         this.setFirst(first);
          this.setSecond(second);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getFirst());
        dataOutput.writeInt(this.getSecond());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.setFirst(dataInput.readUTF());
        this.setSecond(dataInput.readInt());
    }

    @Override
    public int compareTo(PairWritable o) {
         int comp = this.getFirst().compareTo(o.getFirst());
         if (0 != comp){
            return comp;
         }
        return this.getSecond() - o.getSecond();
    }
    /** A Comparator optimized  不用反序列化 直接比较 优化性能 */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(PairWritable.class);
        }
        /**
         * Compare the buffers in serialized form.
         * 字节数组 偏移量 畅读
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1,s1,l1,b2,s2,l2);
        }
    }
    static {                                        // register this comparator
        WritableComparator.define(PairWritable.class, new PairWritable.Comparator());
    }

}
