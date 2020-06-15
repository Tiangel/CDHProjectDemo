package com.cloudera.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * 访问HBase客户端工具类
 */
public class ClientUtils {

    /**
     * 初始化访问HBase访问
     */
    public static Configuration initHBaseENV() {
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.addResource(new Path("./core-site.xml"));
            configuration.addResource(new Path("./hdfs-site.xml"));
            configuration.addResource(new Path("./hbase-site.xml"));

            return  configuration;
        } catch(Exception e) {
            e.printStackTrace();
        }

        return  null;
    }
}
