package com.cloudera.hbase.demo;


import com.cloudera.hbase.utils.ClientUtils;
import com.cloudera.hbase.utils.HBaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseSample {
    public static void main(String[] args) {
        try {
            Configuration configuration = ClientUtils.initHBaseENV();
            Connection connection = HBaseUtils.initConn(configuration);
            if(connection == null) {
                System.exit(1);
            }
            //获取HBase库中所有的表
            HBaseUtils.listTables(connection);

            HBaseUtils.readTable("HDB:SHARE_BUILDING_DETAILS", connection);
            //释放连接
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
