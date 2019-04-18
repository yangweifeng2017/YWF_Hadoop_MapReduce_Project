package com.ywf.parquet;

import com.ywf.interfaces.YWFModel;
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
 * 运行方式与参数: com.ywf.parquet.HDFSReadAndWriter
 * Author yangweifeng
 * Date 2019-04-17 19:18
 * Version 1.0
 **/
public class HDFSReadAndWriter implements YWFModel {
    @Override
    public void execute(String[] args) throws Exception {
        /*
        List<Group> list = readParquetFromHDFS("/user/hive/warehouse/angela.db/t_books_vip_summary_for_cp/ds=2019-03");
        for (Group group : list){
           System.out.println(group);
        }
        writeParquetToHDFS("/user/weifeng/in/parquertData1/yangweifeng.parquet");
        */
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
            writer = new ParquetWriter<Group>(path,ParquetFileWriter.Mode.CREATE,writeSupport,CompressionCodecName.UNCOMPRESSED,128*1024*1024,5*1024*1024,5*1024*1024,ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,ParquetWriter.DEFAULT_WRITER_VERSION,ParquetSchemaCreateFactory.getConfiguration());
            Random random = new Random();
            for(int i=0; i<10; i++){
                // 4. 构建parquet数据，封装成group
                Group group = new SimpleGroupFactory(messageType).newGroup();
                group.append("name", i+"@qq.com")
                        .append("id",i+"@id")
                        .append("age",i)
                        .addGroup("group1")
                        .append("test1", "test1"+i)
                        .append("test2","test2"+i);
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
        try {
             for (Path path : listPath){
                 reader = ParquetReader.builder(groupReadSupport, path).build();
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
