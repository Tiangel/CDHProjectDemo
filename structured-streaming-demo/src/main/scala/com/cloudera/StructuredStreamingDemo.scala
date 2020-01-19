package com.cloudera

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 首先写 个程序来监昕网络端口发来的内容，然后进行 WordCount
  */
object StructuredStreamingDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[4]")
      .setAppName("Structured Network Count")
      .set("fs.defaultFS","file://E:/CDHProjectDemo/spark-demo/temp/")

    // 创建程序入口 SparkSession，并引入 spark.implicits 来允许 Scalaobject 隐式转换为 DataFrame
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    // 第二步: 创建流。配置从 socket 读取流数据，地址和端口为 localhost: 9999
    val lines: DataFrame = spark.readStream.format("socket")
    .option("host", "10.101.71.43")
    .option("port", "9999")
    .load()

    import spark.implicits._
    // 第三步: 进行单词统计。这里 lines 是 DataFrame ，使用 as[String］给它定义类型转换为 Dataset, 之后在 Dataset 里进行单词统计。
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
    val wordcount: DataFrame = words.groupBy("value").count()


    // 第四步: 创建查询句柄，定义打印结果方式并启动程序 这里使用 writeStream 方法, 输出模式为全部输出到控制台。
    val query: StreamingQuery = wordcount.writeStream
      .outputMode(OutputMode.Complete)
      .format("console")
      .start()
    // 调用 awaitTermination 方法来防止程序在处理数据时停止
    query.awaitTermination()

  }

  case class TimeWord(word: String, timestamp: Timestamp)
}
