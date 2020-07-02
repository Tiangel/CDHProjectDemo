package com.cloudera.kudu;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;


public class KuduApiMain {

    public static void main(String[] args) {
        /*
         * 通过kerberos 认证
         * */
        KuduKerberosAuth.initKerberosENV(false);
        /*
         * 获取kudu客户端
         * */
        KuduClient client= GetKuduClient.getKuduClient();
        /*
         * 查询表中字段
         * */
        createTableData(client,"kudu_test.test1");
      //  kam.getTableData(client,"kudutest","zhckudutest1","id");
        /*
         * 创建一个表名
         * */
//        KuduApiTest.createTableData(client,"zhckudutest1");
        /*
         *列出kudu下的所有表
         * */
//        KuduApiTest.tableListShow(client);
        /*
         * 向指定的kudu表中upsert数据
         * */
//        KuduApiTest.upsertTableData(client,"zhckudutest1",10);
        /*
         * 删除kudu表
         * */
//        KuduApiTest.dropTableData(client,"zhckudutest");
    }
    public static void createTableData(KuduClient client, String tableName) {
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        //在添加列时可以指定每一列的压缩格式
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).key(true).
                compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).
                compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        Schema schema = new Schema(columns);
        CreateTableOptions createTableOptions = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<String>();
        hashKeys.add("id");
        int numBuckets = 8;
        createTableOptions.addHashPartitions(hashKeys, numBuckets);

        try {
            if (!client.tableExists(tableName)) {
                client.createTable(tableName, schema, createTableOptions);
            }
            System.out.println("成功创建Kudu表：" + tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    public static void getTableData(KuduClient client, String database, String table, String columns) {
        try {
            KuduTable kudutable = client.openTable( database+"."+table);
            KuduScanner kuduScanner = client.newScannerBuilder(kudutable).build();
            while (kuduScanner.hasMoreRows()) {
                RowResultIterator rowResultIterator = kuduScanner.nextRows();
                while (rowResultIterator.hasNext()) {
                    RowResult rowResult = rowResultIterator.next();
                    System.out.println(rowResult.getString(columns));
                }
            }
            try {
                client.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
