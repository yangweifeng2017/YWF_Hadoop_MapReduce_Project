package com.ywf.parquet;

import com.ywf.interfaces.YWFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.*;

import java.io.IOException;

/**
 * ClassName ParquetWriterMR
 * 功能: TODO
 * 运行方式与参数: TODO
 * Author yangweifeng
 * Date 2019-04-17 17:07
 * Version 1.0
 **/
public class ParquetWriterMR implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
        Configuration conf = new Configuration();
        /*
        写入数据要创建schema
        message hive_schema {
            optional int32 agent_id;
            optional binary agent_name (UTF8);
            optional int32 product_id;
            optional binary product_name (UTF8);
            optional binary value (UTF8);
            optional int32 type;
          }
        //创建方法1
        MessageType schema1 = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY.INT32).as(OriginalType.INT_32).named("agent_id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("agent_name")
                .repeated(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.INT_32).named("product_id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("product_name")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("value")
                .required(PrimitiveType.PrimitiveTypeName.INT32).as(OriginalType.INT_32).named("type")
                .named("hive_schema");*/
        //创建方法2
        MessageType schema = MessageTypeParser.parseMessageType("message hive_schema {\n" +
                        " required int32 agent_id;\n" +
                        " required binary agent_name (UTF8);\n" +
                        " required int32 product_id ;\n" +
                        " required binary product_name (UTF8);\n"+
                         "required group family { \n" +
                            " required binary value (UTF8);\n"+
                            " required int32 type;\n"+
                           "}\n"+
                        "}");
        GroupWriteSupport.setSchema(schema, conf);
        Job job = Job.getInstance(conf, "ParquetReaderAndWriteMRDemo");
        job.setJarByClass(ParquetWriterMR.class);
        job.setMapperClass(ParquetWriteMapper.class);
        job.setNumReduceTasks(0);
       // FileInputFormat.setInputPaths(job,otherargs[0]);
        FileInputFormat.addInputPath(job,new Path("hdfs://nameservice1/user/hive/warehouse/report_data.db/users_distribution/2019-04-16/EasouWejuan_LengthOfTime_2019-04-16"));
        //parquet输出
        job.setOutputFormatClass(ParquetOutputFormat.class);
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);
        Path outPath = new Path("/user/weifeng/in/parquertData1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath);
        }
        FileOutputFormat.setOutputPath(job, outPath);
        job.waitForCompletion(true);
    }
    public static class ParquetWriteMapper extends Mapper<LongWritable, Text, Void, Group> {
        SimpleGroupFactory factory = null;
        protected void setup(Context context) {
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
        }
        public void map(LongWritable _key, Text ivalue, Context context) throws IOException, InterruptedException {
            Group parquetData = factory.newGroup();
            parquetData.append("agent_id",1).append("agent_name","yangweifeng").append("product_id",100001)
                    .append("product_name","hhahha哈");
            parquetData.addGroup("family").append("value","111").append("type",200);
            context.write(null,parquetData);
        }
    }
}
