package com.cloudera.flink.window

import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingWindowDemo extends App {
  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


  val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)


  // Sliding Window
  val slidingWindow1: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
//        .timeWindow(Time.hours(1), Time.minutes(10))
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .sum(1)
    .print("Sliding Window")

  val slidingWindow2: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
//    .timeWindow(Time.seconds(10), Time.seconds(5))
    .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10)))
    .sum(1)
    .print("Sliding Window")


  streamEnv.execute("Sliding Window Demo Execute")

}
