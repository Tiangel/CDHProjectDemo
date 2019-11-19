package com.cloudera.phoenixdemo.controller;

import com.alibaba.fastjson.JSONObject;
import com.cloudera.phoenixdemo.dao.phoenix.CommDataMapper;
import com.cloudera.phoenixdemo.entity.QueryParam;
import com.cloudera.phoenixdemo.utils.HbaseTools;
import com.cloudera.phoenixdemo.utils.PageModel;
import com.cloudera.phoenixdemo.utils.ResponseDto;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@RequestMapping("/api/hdata")
public class CommHbaseController {

    @Autowired
    private CommDataMapper commDataMapper;
    @Autowired
    private HbaseTools hbaseTools;

    @PostMapping("/getHbaseData")
    public ResponseDto getHbaseData(@RequestBody QueryParam queryParam)  throws IOException {
        //try{
            // 查询表字段
//            List<Map<String,Object>> scheme = commDataMapper.dataScheme(queryParam.getTable_name());
//            System.out.println(scheme);
//            List<String> tableScheme = new ArrayList<>();
//            for (Map<String,Object> sh: scheme) {
//                for (Map.Entry entry : sh.entrySet()) {
//                    if (!tableScheme.contains(entry.getKey())) {
//                        tableScheme.add(entry.getKey().toString());
//                    }
//                }
//            }
            //System.out.println(tableScheme);
            // 构建查询字段
            String selectColumn = "";
            if (queryParam.getTable_column().length > 0) {
                for (JSONObject jsonObject: queryParam.getTable_column()) {
                    //selectColumn+=jsonObject.get("column_name") + ",";
                    // 只构建表字段存在的
//                    if (tableScheme.contains(jsonObject.get("column_name").toString().toUpperCase())) {
//                        selectColumn+=jsonObject.get("column_name") + ",";
//                    }
                    try{
                        Object object = commDataMapper.dataScheme(queryParam.getTable_name(),jsonObject.get("column_name").toString());
                        selectColumn+=jsonObject.get("column_name") + ",";

                    }catch (Exception e) {
                        System.out.println(e.getMessage());
                    }


                }
            }
            System.out.println(selectColumn);
            // todo 去除关键字，防止注入

            // 构建查询条件
            String selectCondition = "";

            if (queryParam.getQuery_condition().length > 0) {
                for (ArrayList arrayList : queryParam.getQuery_condition()) {

                    // 根据关键字构建sql 查询条件语句
                    if (arrayList.get(1) != null) {
                        switch (arrayList.get(1).toString()){
                            case "eq":  // 完全相等
                                selectCondition += arrayList.get(0) + " = '" + arrayList.get(2) + "' AND ";
                                break;
                            case "like":  // 模糊匹配
                                selectCondition += arrayList.get(0) + " LIKE '%" + arrayList.get(2) + "%' AND ";
                                break;
                            case "between":  // 时间范围
                                String endDate = arrayList.size() > 3 && arrayList.get(3).toString() != "" ? arrayList.get(3).toString() : new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ").format(new Date());
                                selectCondition += arrayList.get(0) + " BETWEEN '" + arrayList.get(2) + "' AND '" + endDate + "' AND ";
                                break;
                            case "in": // 范围
                                // 格式化参数
                                String inParam = " (";
                                for (String param: arrayList.get(2).toString().split(",")) {
                                    inParam += "'" + param + "', ";
                                }
                                inParam = inParam.substring(0,inParam.length()-2) + ")";
                                //System.out.println(inParam);
                                selectCondition += arrayList.get(0) + " in " + inParam + " AND ";
                                break;
                            case "desc": // 降序
                                // 去除多余AND
                                if (selectCondition.length() > 0) {
                                    selectCondition = selectCondition.substring(0,selectCondition.length()-4);
                                }
                                selectCondition += " ORDER BY " + arrayList.get(0) + " DESC AND ";
                                break;
                            case "asc":  // 升序
                                // 去除多余AND
                                if (selectCondition.length() > 0) {
                                    selectCondition = selectCondition.substring(0,selectCondition.length()-4);
                                }
                                selectCondition += " ORDER BY " + arrayList.get(0) + " ASC AND ";
                                break;
                        }
                    }
                }
                selectCondition = selectCondition.substring(0,selectCondition.length() -4 );
            }

            // 查找数据
            PageHelper.startPage(Integer.parseInt(queryParam.getQuery_page()), Integer.parseInt(queryParam.getQuery_page_size()));
            String colums = "*";
            if (selectColumn.length() > 0) {
                colums = selectColumn.substring(0,selectColumn.length()-1);
            }
            //System.out.println(queryParam.getPrimary_key());
            // 查询rowkey
//            List<Map<String,Object>> rowkey = commDataMapper.selectRowkey(queryParam.getTable_name(), selectCondition,queryParam.getPrimary_key());
//            String  rowkeys = "";
//            for (Map<String,Object> map : rowkey) {
//                System.out.println(map);
//                if (map.get("KEYS")!=null) {
//                    rowkeys+= "'" + map.get("KEYS").toString() + "',";
//                }
//            }
//            System.out.println(rowkeys);
//            System.out.println(selectCondition);
//            if (!rowkeys.equals("")){
//                selectCondition += " AND rowkey in (" + rowkeys.substring(0,rowkeys.length()-1) + ")";
//            }
       // System.out.println(colums);
            List<Map<String,Object>> data = commDataMapper.selectData(queryParam.getTable_name(), colums, selectCondition,queryParam.getPrimary_key());

            // 分页数据返回
            PageInfo<Map<String,Object>> pageInfo = new PageInfo<Map<String,Object>>(data);
//            for (int k=0; k< pageInfo.getList().size();k++) {
//                System.out.println(k);
//                System.out.println( pageInfo.getList().get(k));
//            }
           // List<Map<String,Object>> newData = pageInfo.getList();

            List<Map<String,Object>> returnData = new ArrayList<>();

            for (int j=0;j<pageInfo.getList().size();j++) {

                Map<String,Object> newMap = new HashMap<>();
                if (pageInfo.getList().get(j) != null) {
                    for (String keys:pageInfo.getList().get(j).keySet()) {
                        //System.out.println(keys);

                        for (JSONObject jsonObject: queryParam.getTable_column()) {
                            //返回指定类型数据
                            if (jsonObject.get("column_name").toString().toUpperCase().equals(keys)) {

                                Object value = pageInfo.getList().get(j).get(keys).toString();

                                switch (jsonObject.get("column_type").toString()){
                                    case "string" :
                                        if (value.equals("null")|| value.equals("")) {
                                            newMap.put(keys,"");
                                        }else{
                                            newMap.put(keys,String.valueOf(value));
                                        }

                                        break;
                                    case "short" :
                                        if (value.equals("null")|| value.equals("")) {
                                            newMap.put(keys,Short.valueOf(""));
                                        }else{
                                            newMap.put(keys,Short.valueOf(value.toString()));
                                        }

                                        break;
                                    case "double" :
                                        if (value.equals("null")|| value.equals("")) {
                                            newMap.put(keys,Double.valueOf(0));
                                        }else{
                                            newMap.put(keys,Double.valueOf(value.toString()));
                                        }

                                        break;
                                    case "int":
                                        if (value.equals("null")|| value.equals("")) {
                                            newMap.put(keys,0);
                                        }else{
                                            newMap.put(keys,Integer.valueOf(value.toString()));
                                        }
                                        break;
                                    case "date":
                                        try{
                                            if (value.equals("null") || value.equals("")) {
                                                newMap.put(keys,null);
                                            }else{
                                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                                Date date = simpleDateFormat.parse(value.toString());
                                                newMap.put(keys,simpleDateFormat.format(date));
                                            }
                                        }catch (ParseException parse) {
                                            return ResponseDto.fallR(parse.getMessage());
                                        }


                                        break;
                                    default:
                                        newMap.put(keys,String.valueOf(value));

                                }


                            }

                        }


                    }
                }

                returnData.add(newMap);

            }

            pageInfo.setList(returnData);
            // 数据返回格式构建
            return new ResponseDto(PageModel.convertToPageModel(pageInfo));
//        }catch (Exception e) {
//            System.out.println(e.getMessage());
//            return ResponseDto.fallR("系统或参数错误");
//        }

    }
}
