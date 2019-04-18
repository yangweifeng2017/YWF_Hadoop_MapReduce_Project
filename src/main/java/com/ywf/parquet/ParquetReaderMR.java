package com.ywf.parquet;

import com.ywf.interfaces.YWFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
/**
 * ClassName ParquetReaderMR
 * 功能: 读取Parquet不需要指定元数据
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2019-04-17 16:25
 * Version 1.0
 **/
public class ParquetReaderMR implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ParquetReaderMR");
        job.setJarByClass(ParquetReaderMR.class);
        job.setMapperClass(ParquetReaderMRMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://nameservice1/user/hive/warehouse/angela.db/t_books_vip_summary_for_cp/ds=2019-03"));
        Path outPath = new Path("/user/weifeng/in/parquertData1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }
    /**
     * Parquet读取
     */
    public static class ParquetReaderMRMapper extends Mapper<Void, Group, Text, NullWritable> {
        Text keyText = new Text();
        public void map(Void _key, Group group, Context context) throws IOException, InterruptedException {
            Integer agent_id = group.getInteger("agent_id",0);
            String agent_name = group.getBinary("agent_name",0).toStringUsingUTF8();
            Integer product_id = group.getInteger("product_id",0);
            String  product_name = group.getBinary("product_name",0).toStringUsingUTF8();
            // value类型为 FIXED_LEN_BYTE_ARRAY 无法进行转换，存储parquet文件时，尽量避免使用这种格式
            Integer type = group.getInteger("type",0);
            keyText.set(agent_id + "," + agent_name + "," + product_id + "," + product_name + "," + "0" + "," + type);
            context.write(keyText,NullWritable.get());
        }
    }
}
