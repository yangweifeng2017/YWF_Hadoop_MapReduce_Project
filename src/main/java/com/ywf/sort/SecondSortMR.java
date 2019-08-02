package com.ywf.sort;
import com.ywf.interfaces.YWFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
/**
 * ClassName SecondSortMR
 * 功能: 二次排序
 * Author yangweifeng
 * Date 2019-08-02 16:42
 * Version 1.0
 **/
public class SecondSortMR implements YWFModel{
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://nameservice1:8020");
        conf.set("yarn.resourcemanager.hostname", "qd01-cloud-master005");
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondSortMR.class);
        job.setMapperClass(SecondSortMRMap.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(SecondSortMRReduce.class);
        job.setNumReduceTasks(10);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setGroupingComparatorClass(FirstGroupingComparator.class);
        FileInputFormat.addInputPath(job, new Path("/user/weifeng/in/book_fromweb/data/")); //数据文件
        Path outPath = new Path("/user/weifeng/in/book_fromweb/res");                       //结果
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("job success");
        } else {
            System.out.println("job fail");
        }
        System.exit(flag ? 0 : 1);
    }


    public static class SecondSortMRMap extends Mapper<LongWritable, Text, PairWritable, IntWritable> {
        PairWritable keyText = new PairWritable();
        IntWritable valueInt = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();
            String []strs = lineValue.split(",");
            keyText.set(strs[0],Integer.parseInt(strs[1]));
            valueInt.set(Integer.parseInt(strs[1]));
            context.write(keyText,valueInt);
        }
    }
    public static class SecondSortMRReduce extends Reducer<PairWritable, IntWritable, Text, IntWritable> {
        Text keyText = new Text();
        @Override
        protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) {
            keyText.set(key.getFirst());
            for (IntWritable value : values){
                try {
                    context.write(keyText,value);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
