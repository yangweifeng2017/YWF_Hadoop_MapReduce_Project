package com.ywf.parquet;

import org.apache.parquet.example.data.Group;

import java.util.List;

/**
 * ClassName ParquetResultData
 * Author yangweifeng
 * Date 2019-04-18 17:54
 * Version 1.0
 **/
public class ParquetResultData {
    private List<Group> listGroup;
    public List<Group> getListGroup() {
        return listGroup;
    }
    public void setListGroup(List<Group> listGroup) {
        this.listGroup = listGroup;
    }
    public String getSchema() {
        return Schema;
    }

    public void setSchema(String schema) {
        Schema = schema;
    }
    private String Schema;
}
