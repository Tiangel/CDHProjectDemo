package com.cloudera.phoenixdemo.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;

@Data
public class QueryParam {

    // 表名
    @NotNull
    private String table_name;

    // 表字段
    @NotNull
    private JSONObject[] table_column;

    // 查询页数
    private String query_page = "1";

    // 每页条数
    private String query_page_size = "20";

    // 查询条件
    private ArrayList[] query_condition;

    // 表主键
    private String primary_key;

}
