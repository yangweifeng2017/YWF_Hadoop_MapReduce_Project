package com.ywf.parquet;

import com.ywf.interfaces.YWFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.util.Iterator;

/**
 * ClassName ReadDataFromParquet_V2
 * 功能: Parquet文件读取方式1
 * Author yangweifeng
 * Date 2019-04-16 16:57
 * Version 1.0
 **/
public class ReadDataFromParquet_V2 implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReadDataFromParquet_V2.class);
        job.setJobName("ReadDataFromParquet_V2");
        job.setMapperClass(ReadDataFromParquet_V2Map.class);
        job.setReducerClass(ReadDataFromParquet_V2Reduce.class);

        GroupReadSupport groupReadSupport = new GroupReadSupport();
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job,groupReadSupport.getClass());

        Path InputPath = new Path("/user/weifeng/in/parquertData");
        ParquetInputFormat.addInputPath(job, InputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outPath = new Path("/user/weifeng/in/parquertData1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }
    /**
     * map
     */
    public static class ReadDataFromParquet_V2Map extends Mapper<Void, Group, LongWritable, Text> {
        protected void map(Void key, Group value, Context context)
                throws IOException, InterruptedException {
            String name = value.getString("name",0);
            int  age = value.getInteger("age",0);
            context.write(new LongWritable(age), new Text(name));
        }
    }

    /**
     * reduce
     */
    public static class  ReadDataFromParquet_V2Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while(iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }
    }
}
