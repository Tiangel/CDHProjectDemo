package com.cloudera.flink.window


import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, SessionWindowTimeGapExtractor, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 规定的时间内如果没有数据活跃接入，则认为窗口结束，然后触发窗口计算结果
 */
object SessionWindowDemo extends App {
  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  //  streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)

  // Session Window
  val sessionWindow1: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    // 通过使用 EventTimeSessionWindows 定义 Event Time 滚动窗口
    .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
    .sum(1)
    .print("Session Window")


  val sessionWindow2: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    // 表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
    .sum(1)
    .print("Session Window")


  // 动态 Session Gap
  val sessionWindow3: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    // 通过使用 EventTimeSessionWindows 定义 Event Time 滚动窗口
    .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Int)] {
      override def extract(element: (String, Int)): Long = {
        System.currentTimeMillis()
      }
    }))
    .sum(1)
    .print("Session Window")


  val sessionWindow4: DataStreamSink[(String, Int)] = socketDs.flatMap(_.split(" ", -1))
    .filter(StringUtils.isNotBlank(_))
    .map((_, 1))
    .keyBy(0)
    // 表示如果 5s 内没出现数据则认为超出会话时长，然后计算这个窗口的和
    .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Int)] {
      override def extract(element: (String, Int)): Long = {
        System.currentTimeMillis()
      }
    }))
    .sum(1)
    .print("Session Window")


  streamEnv.execute("Session Window Demo Execute")

}
