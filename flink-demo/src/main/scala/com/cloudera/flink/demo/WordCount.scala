package com.cloudera.flink.demo

import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.streaming.api.functions.sink.SocketClientSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val host: String = "bigdata-dev-kafka-01"
    val port: Int = 7777

//    val inputPath = "E:\\CDHProjectDemo\\flink-demo\\data\\test\\test.txt"
//    val testFileDs: DataStream[String] = env.readTextFile(inputPath)

    val socketDs: DataStream[String] = env.socketTextStream(host, port)


    val wordCount: DataStream[(String, Int)] = socketDs.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    //    wordCount.print()

    wordCount.addSink(new SocketClientSink("127.0.0.1", 6666, new SerializationSchema[(String, Int)]() {
      override def serialize(element: (String, Int)): Array[Byte] ={
        val bytes: Array[Byte] = (element._1 + "_" + element._2).getBytes("utf-8")
        bytes
      }
    },3,true))
    env.execute("Socket stream word count")
  }

}
