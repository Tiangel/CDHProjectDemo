package com.cloudera.writeToOut

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaveData2File {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setIfMissing("spark.master", "local[*]")
      .setAppName("SaveData2File_SparkStreaming")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(3))
//    E:\CDHProjectDemo\spark-streaming-demo\data
//    spark-streaming-demo/data
    val input =  "hdfs://10.101.71.54:8020/temp/data/input"
    val output = "file://E:\\CDHProjectDemo\\spark-streaming-demo\\data\\output"

    println("read file name: " + input + "\nout file name: " + output)

    // 从磁盘上读取文本文件作为输入流
    val textStream = streamingContext.textFileStream(input)
    // 进行词频统计
    val wcStream = textStream.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    // 打印到控制台并保存为文本文件和序列化文件
    wcStream.print()
    wcStream.saveAsTextFiles(output + "\\saveAsTextFiles")
    wcStream.saveAsObjectFiles(output + "\\saveAsObjectFiles")

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
