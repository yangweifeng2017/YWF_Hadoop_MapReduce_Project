package com.ywf.sort;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * ClassName FirstGroupingComparator
 * 功能: 分组
 * Author yangweifeng
 * Date 2019-08-02 17:36
 * Version 1.0
 **/
public class FirstGroupingComparator implements RawComparator<PairWritable> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return  WritableComparator.compareBytes(b1,s1,l1 - 4,b2,s2,l2 - 4);
    }
    @Override
    public int compare(PairWritable o1, PairWritable o2) {
        return o1.getFirst().compareTo(o2.getFirst());
    }
}
