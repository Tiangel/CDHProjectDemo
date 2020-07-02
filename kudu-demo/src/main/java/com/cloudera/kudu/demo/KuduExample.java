package com.cloudera.kudu.demo;

import com.cloudera.utils.KuduUtils;
import org.apache.kudu.client.*;

/**
 * 使用API方式访问Kudu数据库
 */
public class KuduExample {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "bigdata-dev-kerberos-01:7051,bigdata-dev-kerberos-02:7051");

    public static void main(String[] args) {
        KuduClient kuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        String tableName = "user_info";

        //删除Kudu的表
        KuduUtils.dropTable(kuduClient, tableName);

        //创建一个Kudu的表
        KuduUtils.createTable(kuduClient, tableName);

        //列出Kudu下所有的表
        KuduUtils.tableList(kuduClient);

        //向Kudu指定的表中插入数据
        KuduUtils.upsert(kuduClient, tableName, 100);

        //扫描Kudu表中数据
        KuduUtils.scanerTable(kuduClient, tableName);

        try {
            kuduClient.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
}
