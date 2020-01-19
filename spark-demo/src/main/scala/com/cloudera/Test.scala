package com.cloudera

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //数据存放的目录；
    var dataPath = "file:\\\\\\E:\\CDHProjectDemo\\spark-demo\\data\\moviedata\\medium\\"

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[4]")
      .setAppName("Test")

    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    //获取 SparkSession 的 SparkContext
    val sc = spark.sparkContext
    val user: RDD[String] = sc.textFile(dataPath + "users.dat")
    user.foreach(println(_))
    sc.stop()
  }
}
