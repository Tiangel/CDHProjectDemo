package com.cloudera.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val host: String = "bigdata-dev-kafka-01"
    val port: Int = 7777

    val inputPath = "E:\\CDHProjectDemo\\flink-demo\\data\\test\\test.txt"
    val testFileDs: DataStream[String] = env.readTextFile(inputPath)

    val socketDs: DataStream[String] = env.socketTextStream(host, port)


    val wordCount: DataStream[(String, Int)] = socketDs.flatMap(_.split(" "))
        .map((_, 1))
        .keyBy(0)
        .sum(1)
    wordCount.print()
    env.execute("Socket stream word count")
  }

}
