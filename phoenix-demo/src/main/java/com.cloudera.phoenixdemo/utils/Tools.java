package com.cloudera.phoenixdemo.utils;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 通用工具类
 */
@Component
public class Tools {

    /**
     * 递归遍历组织关系
     * @param object
     * @param result
     * @param pid
     * @return
     * @throws JSONException
     */
    public static List traversalOrJson(Object object, List result, String pid) throws JSONException {
        if (object == null) {
            return null;
        }
        if (object instanceof JSONObject) {
            // json对象返回json
            JSONObject retJsonObj = new JSONObject();
            JSONObject jsonObject = (JSONObject) object;
            for (Map.Entry entry : jsonObject.entrySet()) {
                JSONObject object1 = JSONObject.parseObject(entry.getValue().toString());
                if (object1.get("childOrganiz") instanceof JSONObject) {
                    String parent = String.valueOf(object1.get("organizId"));
                    traversalOrJson(object1.get("childOrganiz"),result,parent);
                }
                //System.out.println(object1);
                //object1.remove("devices");
                object1.remove("childOrganiz");
                if (!result.contains(retJsonObj)) {
                    result.add(retJsonObj);
                }

            }
        } else if (object instanceof JSONArray){
            // array返回array
            JSONObject retJsonObj = new JSONObject();
            JSONArray jsonArray = (JSONArray) object;
            for (int i=0;i<jsonArray.size();i++) {
                JSONObject object1 = JSONObject.parseObject(jsonArray.get(i).toString());
                if (object1.get("childOrganiz") instanceof JSONArray) {
                    String parent = String.valueOf(object1.get("organizId"));
                    traversalOrJson(object1.get("childOrganiz"),result,parent);
                }
                //object1.remove("devices");
                object1.remove("childOrganiz");
                object1.put("parentOrganizId",pid);
                if (!result.contains(retJsonObj)) {
                    result.add(object1);
                }
            }
        } else {
            return result;
        }
        return result;
    }

    /**
     * 连接hbase
     * @return
     * @throws IOException
     */
    public static Connection initHbase(String hbaseClientPort,String hbaseQuorum ,String hbaseMaster) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        System.out.println(hbaseClientPort);
        System.out.println(hbaseQuorum);
        System.out.println(hbaseMaster);
        configuration.set("hbase.zookeeper.property.clientPort", hbaseClientPort);
        configuration.set("hbase.zookeeper.quorum", hbaseQuorum);
        configuration.set("hbase.master", hbaseMaster);
        configuration.setInt("hbase.rpc.timeout",60000);
        configuration.setInt("hbase.client.operation.timeout",60000);
        configuration.setInt("hbase.client.scanner.timeout.period",60000);
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println(connection);

        return connection;
    }
}
