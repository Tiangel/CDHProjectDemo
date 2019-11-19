package com.cloudera.vms.jobs;//package com.izhonghong.vms.jobs;
//
//import com.izhonghong.vms.conf.DTO;
//import com.tyaer.database.mysql.MySQLHelperPool;
//import org.apache.log4j.Logger;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.sql.SQLContext;
//
//
///**
// * 计算影响力指数
// * Created by Twin on 2018/9/26.
// */
//public class CalculateInfluenceIndexJob {
//    private static final Logger logger = Logger.getLogger(CalculateInfluenceIndexJob.class);
//    private static MySQLHelperPool amsPool;
//
//    static {
//        amsPool = new MySQLHelperPool(DTO.configFileReader.getConfigValue("jdbc.mysql.username"),
//                DTO.configFileReader.getConfigValue("jdbc.mysql.password"),
//                DTO.configFileReader.getConfigValue("jdbc.mysql.url"));
//
//    }
//
//    String sql = "SELECT organizationid,id FROM ams.vt_subject WHERE organizationid=10006 and type=10";
//    SparkConf conf = new SparkConf().setAppName("CalculateInfluenceIndexJob");
//    SparkContext sc = new SparkContext(conf);
//    SQLContext sqlContext = new SQLContext(sc);
//
//
//}
