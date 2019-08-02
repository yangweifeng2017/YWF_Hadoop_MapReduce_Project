package com.ywf.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * ClassName FirstPartitioner
 * 功能: 自定义分区函数
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2019-08-02 17:09
 * Version 1.0
 **/
public class FirstPartitioner extends Partitioner<PairWritable,IntWritable> {
    @Override
    public int getPartition(PairWritable key, IntWritable value, int numPartitions) {
        return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
