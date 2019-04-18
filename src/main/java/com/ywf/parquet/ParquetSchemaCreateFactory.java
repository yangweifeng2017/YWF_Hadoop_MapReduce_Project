package com.ywf.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

/**
 * ClassName ParquetSchemaCreateFactory
 * Author yangweifeng
 * Date 2019-04-18 17:31
 * Version 1.0
 **/
public class ParquetSchemaCreateFactory {
    private static Configuration configuration = new Configuration();
    static {
        configuration.set("fs.defaultFS", "hdfs://10.26.29.210:8020");
        configuration.set("yarn.resourcemanager.hostname", "qd01-cloud-master005");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    }
    /**
     * 获取 hadoop Configuration配置文件
     * @return Configuration
     */
    public static Configuration getConfiguration(){
        return configuration;
    }
    public static MessageType getDemoSchema() {
        return Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("gid")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("agent_id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("agent_name")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("product_id")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("product_name")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("date_type")
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("value")
                .named("t_books_vip_splitmoney_for_cp_Schema");
    }
    public static MessageType getMessageTypeFromCode(){
        MessageType messageType =
                Types.buildMessage()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("id")
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("name")
                        .required(PrimitiveType.PrimitiveTypeName.INT32).named("age")
                        .requiredGroup()
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test1")
                        .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("test2")
                        .named("group1")
                        .named("trigger");
        return messageType;
    }
}
