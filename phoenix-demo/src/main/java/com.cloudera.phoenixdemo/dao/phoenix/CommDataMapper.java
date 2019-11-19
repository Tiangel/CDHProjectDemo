package com.cloudera.phoenixdemo.dao.phoenix;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface CommDataMapper {

    List<Map<String,Object>> selectData(@Param("tableName") String tableName, @Param("columns") String columns, @Param("condition") String condition, @Param("p_key") String primary_key);
    List<Map<String,Object>> selectRowkey(@Param("tableName") String tableName, @Param("condition") String condition, @Param("p_key") String primary_key);
    List<Map<String,Object>> dataScheme(@Param("tableName") String tableName, @Param("col") String colName);
}
