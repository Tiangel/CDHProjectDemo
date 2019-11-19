package com.cloudera.phoenixdemo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class HbaseTools {
    public  static Connection connection;
    private static Configuration configuration;
    private static HbaseTools hbaseTools;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String hbaseClientPort;

    @Value("${hbase.zookeeper.quorum}")
    private String hbaseQuorum;

    @Value("${hbase.master}")
    private String hbaseMaster;

    /**
     * 创建连接 保证只有一个连接被创建
     */
    public void init() {
        System.out.println(hbaseClientPort + hbaseQuorum + hbaseMaster);
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
        }
        try{
            if (hbaseClientPort!=null && hbaseQuorum!=null && hbaseMaster!=null) {
                configuration.set("hbase.zookeeper.property.clientPort", hbaseClientPort);
                configuration.set("hbase.zookeeper.quorum", hbaseQuorum);
                configuration.set("hbase.master", hbaseMaster);
                configuration.setInt("hbase.rpc.timeout",60000);
                configuration.setInt("hbase.client.operation.timeout",600000);
                configuration.setInt("hbase.client.scanner.timeout.period",600000);
                if (connection ==null || connection.isClosed()) {
                    connection = ConnectionFactory.createConnection(configuration);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 实例
     * @return
     */
    public  HbaseTools getInstance() {
        System.out.println(hbaseClientPort + hbaseQuorum + hbaseMaster);
        synchronized (HbaseTools.class) {
            if (hbaseTools == null) {
                this.init();
            }
        }
        return hbaseTools;
    }

    /**
     * 获取数据表
     * @param tableName
     * @return
     * @throws IOException
     */
    public  Table getTable(String tableName) throws IOException{
        System.out.println(connection);
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table;
    }

    /**
     * 获取连接
     * @return
     * @throws IOException
     */
    public Connection getConnection () {
        return connection;
    }


}
