package com.ywf.parquet;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * ClassName WriteDataToParquet
 * 功能: 写Parquet格式数据
 * 运行方式与参数: hadoop jar ./EasouJobs-1.0-SNAPSHOT-jar-with-dependencies.jar com.easou.mapreduce.parquet.WriteDataToParquet
 * Author yangweifeng
 * Date 2018/11/13 15:30
 * Version 1.0
 **/
public class WriteDataToParquet implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String writeSchema = "message example {\n" +
                "required binary name;\n" +
                "required int32 age;\n" +
                "}";
        conf.set("parquet.example.schema",writeSchema);
        Job job = Job.getInstance(conf);
        job.setJobName("WriteDataToParquetMR");
        job.setJarByClass(WriteDataToParquet.class);
        String in = "/user/weifeng/in/words";
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Group.class);
        job.setMapperClass(WriteDataToParquetMap.class);
        job.setReducerClass(WriteDataToParquetReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(ParquetOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(in));
        job.setNumReduceTasks(1);
        Path outPath = new Path("/user/weifeng/in/parquertData1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath);
        }
        ParquetOutputFormat.setOutputPath(job, outPath);
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        job.waitForCompletion(true);
    }

    /**
     * map
     */
    public static class WriteDataToParquetMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while (token.hasMoreTokens()) {
                word.set(token.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * reduce
     */
    public static class WriteDataToParquetReduce extends Reducer<Text, IntWritable, Void, Group> {
        private SimpleGroupFactory factory;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Group group = factory.newGroup()
                    .append("name",  key.toString())
                    .append("age", sum);

            context.write(null,group);
        }
    }
}