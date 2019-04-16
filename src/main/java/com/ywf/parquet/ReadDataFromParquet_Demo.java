package com.ywf.parquet;

import com.ywf.interfaces.YWFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;


/**
 * ClassName ReadDataFromParquet
 * 功能: 读取Parquet格式数据
 * 运行方式与参数: hadoop jar ./EasouJobs-1.0-SNAPSHOT-jar-with-dependencies.jar com.easou.mapreduce.parquet.ReadDataFromParquet_Demo
 * Author yangweifeng
 * Date 2018/11/13 15:30
 * Version 1.0
 **/
public class ReadDataFromParquet_Demo implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(ReadDataFromParquet_Demo.class);
        job.setJobName("ReadDataFromParquet_Demo");
        job.setMapperClass(ReadDataFromParquet_DemoMap.class);
       // job.setReducerClass(ReadDataFromParquetReduce.class);
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        job.setInputFormatClass(ParquetInputFormat.class);
        ParquetInputFormat.setReadSupportClass(job,groupReadSupport.getClass());
        Path InputPath = new Path("hdfs://nameservice1/user/hive/warehouse/angela.db/t_books_vip_summary_for_cp/ds=2018-06");
        ParquetInputFormat.addInputPath(job, InputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
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
    public class ReadDataFromParquet_DemoMap extends Mapper<Void, Group, Text, NullWritable> {
        Text keyText = new Text();
        protected void map(Void key, Group value, Context context) {
            try{
                int agent_id = value.getInteger("agent_id",0);
                String agent_name = value.getString("agent_name",0);
                int product_id = value.getInteger("product_id",0);
                String product_name = value.getString("product_name",0);
                int type = value.getInteger("type",0);
                keyText.set(agent_id + "," + agent_name + "," + product_id + "," + product_name  + "," + type);
                context.write(keyText, NullWritable.get());
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
    /**
     * reduce

    public static class  ReadDataFromParquetReduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            while(iterator.hasNext()){
                context.write(key,iterator.next());
            }
        }
    }
     */
}
