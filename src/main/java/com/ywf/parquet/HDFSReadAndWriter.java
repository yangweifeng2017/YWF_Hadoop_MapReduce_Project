package com.ywf.parquet;

import com.ywf.interfaces.YWFModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.*;
import java.io.IOException;
import java.util.*;

/**
 * ClassName HDFSReadAndWriter
 * 运行方式与参数: java -jar YWF_Hadoop_MapReduce_Project-1.0-jar-with-dependencies.jar com.ywf.parquet.HDFSReadAndWriter /user/hive/warehouse/event_info_v2.db/easou_log/ds=2018-01-31/000218_0
 * Author yangweifeng
 *
 * Date 2019-04-17 19:18
 * Version 1.0
 **/
public class HDFSReadAndWriter implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
         /*
          //map类型读取
        ParquetResultData parquetResultData = readParquetFromHDFS(args[0]);
        for (Group group : parquetResultData.getListGroup()){
            Group event_paralist = group.getGroup("event_paralist",0);
            System.out.println(event_paralist);
            //获取重复次数
            Integer num  = event_paralist.getFieldRepetitionCount("map");
            System.out.println("重复次数" + num);
            for (int i = 0; i < num; i++) {
                Group group1 = event_paralist.getGroup("map",i); //第二个参数代表重复的位置index
                System.out.println(i + "," + group1.getBinary("key",0).toStringUsingUTF8() + "," + group1.getBinary("value",0).toStringUsingUTF8());
            }
            break;
        }
         */
        //-----------------------
       // writeParquetToHDFS("/user/weifeng/in/parquertData1/yangweifeng.parquet");
        writeParquetToRepeatedHDFS("/user/weifeng/in/parquertData1/map.parquet");
        /*
        List<HashMap<String, String>> hashMapfile = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            HashMap<String, String> hashMap = new HashMap<>();
            hashMap.put("gid", String.valueOf(i + 100000000));
            hashMap.put("agent_id", String.valueOf(i + 1));
            hashMap.put("agent_name", "阅文");
            hashMap.put("product_id", String.valueOf(10001));
            hashMap.put("product_name", "宜搜小说");
            hashMap.put("date_type", "1");
            hashMap.put("value", "269411.25");
            hashMapfile.add(hashMap);
        }
        writeParquetToHDFS(hashMapfile,"/user/weifeng/in/parquertData1");
         */
    }

    /**
     * 将数据写入到HDFS ParquetFile中
     * @param hashMapfile
     * @param filepath
     * @throws IOException
     */
    public static void writeParquetToHDFS(List<HashMap<String, String>> hashMapfile, String filepath)
            throws IOException {
        MessageType messageType = ParquetSchemaCreateFactory.getDemoSchema(); // 获取元数据信息
        if (null == messageType)
            return;
        Path parquetFile = new Path(filepath + "/data.parquet");
        FileSystem fs = FileSystem.get(ParquetSchemaCreateFactory.getConfiguration());
        fs.deleteOnExit(parquetFile);
        fs.createNewFile(parquetFile);
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(messageType);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(messageType, ParquetSchemaCreateFactory.getConfiguration());
        ParquetWriter<Group> parquetWriter =  new ParquetWriter<>(parquetFile,ParquetFileWriter.Mode.OVERWRITE,writeSupport,CompressionCodecName.UNCOMPRESSED,128*1024*1024,5*1024*1024,5*1024*1024,ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,ParquetWriter.DEFAULT_WRITER_VERSION,ParquetSchemaCreateFactory.getConfiguration());
        for (HashMap<String, String> singleRecord : hashMapfile) {
            Group group = simpleGroupFactory.newGroup();
            for (Map.Entry<String, String> entry : singleRecord.entrySet()) {
                group.append(entry.getKey(), entry.getValue());
            }
            parquetWriter.write(group);
        }
        parquetWriter.close();
    }
    /**
     * HDFS文件写入
     * @param filePath
     */
    public static void writeParquetToHDFS(String filePath){
        // 1. 声明parquet的messageType
        MessageType messageType = ParquetSchemaCreateFactory.getMessageTypeFromCode();
        System.out.println(messageType.toString());
        // 2. 声明parquetWriter
        Path path = new Path(filePath);
        GroupWriteSupport.setSchema(messageType, ParquetSchemaCreateFactory.getConfiguration());
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        // 3. 写数据
        ParquetWriter<Group> writer = null;
        try {
            writer = new ParquetWriter<Group>(path,ParquetFileWriter.Mode.OVERWRITE,writeSupport,CompressionCodecName.UNCOMPRESSED,128*1024*1024,5*1024*1024,5*1024*1024,ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,ParquetWriter.DEFAULT_WRITER_VERSION,ParquetSchemaCreateFactory.getConfiguration());
            Random random = new Random();
            for(int i=0; i<10; i++){
                // 4. 构建parquet数据，封装成group
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name", i+"@qq.com")
                        .append("id",i+"@id")
                        .append("age",i)
                        .addGroup("group1")
                        .append("test1", "test1"+i)
                        .append("test2", "test2"+i);
                writer.write(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(writer != null){
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * HDFS文件写入
     * @param filePath
     */
    public static void writeParquetToRepeatedHDFS(String filePath){
        // 1. 声明parquet的messageType
        MessageType messageType = ParquetSchemaCreateFactory.getMessageTypeFromStringCode();
        System.out.println(messageType.toString());
        // 2. 声明parquetWriter
        Path path = new Path(filePath);
        GroupWriteSupport.setSchema(messageType, ParquetSchemaCreateFactory.getConfiguration());
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        // 3. 写数据
        ParquetWriter<Group> writer = null;
        try {
            writer = new ParquetWriter<Group>(path,ParquetFileWriter.Mode.CREATE,writeSupport,CompressionCodecName.UNCOMPRESSED,128*1024*1024,5*1024*1024,5*1024*1024,ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,ParquetWriter.DEFAULT_WRITER_VERSION,ParquetSchemaCreateFactory.getConfiguration());
            Random random = new Random();
            for(int i=0; i<10; i++){
                // 4. 构建parquet数据，封装成group
                Group group = new SimpleGroupFactory(messageType).newGroup();
                /*
                Group event_paralist = new SimpleGroupFactory(ParquetSchemaCreateFactory.getMessageTypeFromStringCode1()).newGroup();
                for (int j = 0; j < 10; j++) {
                    Group map = new SimpleGroupFactory(ParquetSchemaCreateFactory.getMessageTypeFromStringCode2()).newGroup();
                    map.add(0,"111");
                    event_paralist.add(j,map);
                }*/
                group.append("os", i+"@qq.com")
                        .append("phone_udid2",i+"@id")
                        .append("phone_softversion","111")
                        .append("last_cpid","111")
                        .append("package_name","111")
                        .append("appkey","111")
                        .append("sdk_version","111")
                        .append("cpid","111")
                        .append("type","111")
                        .append("phone_imei","111")
                        .append("phone_apn","111")
                        .append("phone_udid","111")
                        .append("gatewayip","111")
                        .append("phone_mac","111")
                        .append("phone_imsi","111")
                        .append("phone_city","111")
                        .append("src_code","111")
                        .append("status","111")
                        .append("time","111")
                        .append("event_id","111")
                        .append("server_time","111")
                        .addGroup("event_paralist")
                        .addGroup("map")
                        .append("key", "test1")
                        .append("value", "test2")
                        .addGroup("map")
                        .append("key", "test1")
                        .append("value", "test2")
                        .addGroup("map")
                        .append("key", "test1")
                        .append("value", "test2");
                writer.write(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(writer != null){
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    /**
     * 读取HDFS文件
     * @param filePath 文件路径
     */
    public static ParquetResultData readParquetFromHDFS(String filePath) throws IOException {

        ParquetResultData parquetResultData = new ParquetResultData();
        List<Group> listGroup = new ArrayList<>();
        // 1. 声明readSupport
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        FileSystem hdfsFs = FileSystem.get(ParquetSchemaCreateFactory.getConfiguration());
        Path Hdfspath = new Path(filePath);
        List<Path> listPath = new ArrayList<>();
        if (hdfsFs.isDirectory(Hdfspath)) {
            FileStatus[] files = hdfsFs.listStatus(Hdfspath);
            for (FileStatus fileStatus : files){
                listPath.add(fileStatus.getPath());
            }
        }else {
            listPath.add(Hdfspath);
        }
        boolean isFirst = true;
        // 2.通过parquetReader读文件
        ParquetReader<Group>reader = null;
        Configuration configuration = ParquetSchemaCreateFactory.getConfiguration();
        // 复杂类型 需要指定 scama
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
                "optional group event_paralist (MAP) {\n" +
                "    repeated group map (MAP_KEY_VALUE) {\n" +
                "      required binary key (UTF8);\n" +
                "      optional binary value (UTF8);\n" +
                "    }\n" +
                "  }\n" +
                "  optional binary server_time (UTF8);\n" +
                "}\n";
        configuration.set("parquet.read.schema",schema);
        try {
             for (Path path : listPath){
                 reader = ParquetReader.builder(groupReadSupport, path).withConf(configuration).build();
                 Group group;
                 while ((group = reader.read()) != null){
                     if (isFirst){
                         List<Type> list1 = group.getType().getFields();
                         StringBuilder stringBuilder = new StringBuilder();
                          for (Type type : list1){
                              stringBuilder.append(type.getName() + ",");
                          }
                         isFirst = false;
                         parquetResultData.setSchema(stringBuilder.substring(0,stringBuilder.length() - 1));
                     }
                     listGroup.add(group);
                 }
             }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(reader != null){
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        parquetResultData.setListGroup(listGroup);
        return parquetResultData;
    }

}
