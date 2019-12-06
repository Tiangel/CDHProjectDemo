package com.cloudera

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WindowOnEventTime {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[4]")
      .setAppName("Structured Network Count")
      .set("fs.defaultFS","file://E:/CDHProjectDemo/spark-demo/temp/")

    // 创建程序入口 SparkSession，并引入 spark.implicits 来允许 Scalaobject 隐式转换为 DataFrame
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._
    // 第二步: 创建流。配置从 socket 读取流数据，地址和端口为 localhost: 9999
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "10.101.71.43")
      .option("port", "9999")
      .load()


    val words = lines.as[(String, Timestamp)].flatMap(line => line._1.split(" ")
      .map(word => TimeWord(word, line._2))).toDF()

    // 假如输入的数据 words 格式是 timestamp: Timestamp. word: String
    import org.apache.spark.sql.functions._
    val windowedCounts = words.groupBy(
    // 设置窗口按照 timestamp 列为参照时间， 10min 为窗口大小，5min 滑动一次，并且按照 word 进行分组计数
      window($"timestamp", "10 seconds", "5 seconds"),
      $"word"
    ).count

    // 第四步: 创建查询句柄，定义打印结果方式并启动程序 这里使用 writeStream 方法, 输出模式为全部输出到控制台。
    val query: StreamingQuery = windowedCounts.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .option("truncate", "false")
      .start()

    // 调用 awaitTermination 方法来防止程序在处理数据时停止
    query.awaitTermination()

  }

  case class TimeWord(word: String, timestamp: Timestamp)
}
