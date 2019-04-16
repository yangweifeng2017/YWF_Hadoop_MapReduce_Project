package com.ywf.parquet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

/**
 * MR Parquet格式数据读写Demo
 */
public class ParquetReaderAndWriteMRDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherargs=new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherargs.length!=3){
            System.out.println("<in> <out> 1");
            System.out.println("<parquet-in> <out> 2");
            System.out.println("<in> <parquet-out> 3");
            System.out.println("<parquet-in> <parquet-out> 4");
            System.exit(2);
        }
        //此demo 输入数据为2列     city  ip
        MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("city")
                .required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("ip")
                .named("pair");
        System.out.println("[schema]=="+schema.toString());
        GroupWriteSupport.setSchema(schema, conf);

        Job job = Job.getInstance(conf, "ParquetReadMR");
        job.setJarByClass(ParquetReaderAndWriteMRDemo.class);

        if(otherargs[2].equals("1")){
            job.setMapperClass(NormalMapper.class);
            job.setReducerClass(NormalReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job,otherargs[0] );
            FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
            if (!job.waitForCompletion(true))
                return;
        }
        if(otherargs[2].equals("3")){
            job.setMapperClass(ParquetWriteMapper.class);
            job.setNumReduceTasks(0);
            FileInputFormat.setInputPaths(job,otherargs[0] );

            //parquet输出
            job.setOutputFormatClass(ParquetOutputFormat.class);
            ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
//            ParquetOutputFormat.setOutputPath(job, new Path(otherargs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
            if (!job.waitForCompletion(true))
                return;
        }

        if(otherargs[2].equals("2")){
            //parquet输入
            job.setMapperClass(ParquetReadMapper.class);
            job.setNumReduceTasks(0);
            job.setInputFormatClass(ParquetInputFormat.class);
            ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.setInputPaths(job,otherargs[0] );
            FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));
            if (!job.waitForCompletion(true))
                return;
        }
        if(otherargs[2].equals("4")){
            //TODO 不想写了
        }
    }

    public static class ParquetWriteMapper extends Mapper<LongWritable, Text, Void, Group> {
        SimpleGroupFactory factory=null;
        protected void setup(Context context) throws IOException ,InterruptedException {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        };
        public void map(LongWritable _key, Text ivalue, Context context) throws IOException, InterruptedException {
            Group pair=factory.newGroup();
            String[] strs=ivalue.toString().split("\\s+");
            pair.append("city", strs[0]);
            pair.append("ip", strs[1]);
            context.write(null,pair);
        }
    }

    public static class ParquetReadMapper extends Mapper<Void, Group, Text, Text> {
        public void map(Void _key, Group group, Context context) throws IOException, InterruptedException {
            String city=group.getString(0, 0);
            String ip=group.getString(1, 0);
            context.write(new Text(city),new Text(ip));
        }
    }

    public static class NormalMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
            String[] strs=ivalue.toString().split("\\s+");
            context.write(new Text(strs[0]), new Text(strs[1]));
        }
    }
    public static class NormalReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(_key,text);
            }

        }
    }

}