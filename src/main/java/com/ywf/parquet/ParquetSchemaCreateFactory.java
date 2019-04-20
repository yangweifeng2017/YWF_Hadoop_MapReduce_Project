package com.ywf.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.*;

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

    public static MessageType getMessageTypeFromStringCode(){
        String schema = "message hive_schema {\n" +
                "optional binary os (UTF8);\n" +
                "optional binary phone_udid2 (UTF8);\n" +
                "optional binary phone_softversion (UTF8);\n" +
                "optional binary last_cpid (UTF8);\n" +
                "optional binary package_name (UTF8);\n" +
                "optional binary appkey (UTF8);\n" +
                "optional binary sdk_version (UTF8);\n" +
                "optional binary cpid (UTF8);\n" +
                "optional binary currentnetworktype (UTF8);\n" +
                "optional binary type (UTF8);\n" +
                "optional binary phone_imei (UTF8);\n" +
                "optional binary phone_apn (UTF8);\n" +
                "optional binary phone_udid (UTF8);\n" +
                "optional binary gatewayip (UTF8);\n" +
                "optional binary phone_mac (UTF8);\n" +
                "optional binary phone_imsi (UTF8);\n" +
                "optional binary phone_city (UTF8);\n" +
                "optional binary src_code (UTF8);\n" +
                "optional binary status (UTF8);\n" +
                "optional binary time (UTF8);\n" +
                "optional binary event_id (UTF8);\n" +
                "optional binary server_time (UTF8);\n" +
                "optional group event_paralist (MAP) {\n" +
                "    repeated group map (MAP_KEY_VALUE) {\n" +
                "      required binary key (UTF8);\n" +
                "      optional binary value (UTF8);\n" +
                "    }\n" +
                "  }\n" +
                "}\n";
        MessageType messageType =  MessageTypeParser.parseMessageType(schema);
        return messageType;
    }
}
