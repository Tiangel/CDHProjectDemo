package com.cloudera.flink.window


import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object RollingWindowDemo extends App {
  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//  streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)

  // Rolling Window
  val rollingWindow1: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    // 取决于 streamEnv.setStreamTimeCharacteristic
//    .timeWindow(Time.seconds(5))
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sum(1)
    .print("Rolling Window")


  // Rolling Window
  val rollingWindow2: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .sum(1)
    .print("Rolling Window")


  streamEnv.execute("Rolling Window Demo Execute")

}
