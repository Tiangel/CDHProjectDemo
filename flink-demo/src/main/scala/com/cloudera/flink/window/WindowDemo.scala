package com.cloudera.flink.window


import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowDemo extends App {

  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


  val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)





  // Count Rolling Window
  val countRollingWindow: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    .countWindow(5)
    .sum(1)
    .print("Count Rolling Window")


  // Count Sliding Window
  val countSlidingWindow: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    .countWindow(10,5)
    .sum(1)
    .print("Count Sliding Window")

  // Session Window
  val sessionWindow: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    // 表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
    .sum(1)
    .print("Session Window")

  streamEnv.execute("Window All Demo Execute")
}
