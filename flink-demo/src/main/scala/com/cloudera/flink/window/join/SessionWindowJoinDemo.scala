package com.cloudera.flink.window.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time


/**
 * Session Window Join 会话窗口关联
 */
object SessionWindowJoinDemo extends App {


  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  //创建黑色元素数据集
  val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 18L), (4, 1L), (5, 4L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.milliseconds(1)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })

  //创建白色元素数据集
  val whiteStream: DataStream[(Int, Long)] = env.fromElements(
    (2, 1L), (2, 2L), (2, 3L), /*(2, 4L),(2, 5L),(2, 6L),(2, 7L),(2, 8L),*/ (2, 9L), (2, 10L),
    (2, 11L), /* (2, 12L),(2, 13L),(2, 14L),(2, 15L),(2, 16L),*/ (2, 17L),  /*(2, 18L),(2, 19L),*/ (2, 20L),
    /*(2, 21L), (2, 22L),(2, 23L), (2, 24L),*/ (2, 25L), (2, 26L), (2, 27L), (2, 28L), /*(2, 29L), (2, 30L),
    (2, 31L), (2, 32L), (2, 33L), (2, 34L),*/ (2, 35L), (2, 36L),
    (5, 3L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.milliseconds(1)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })

  //通过Join方法将两个数据集进行关联
  val windowStream: DataStream[(Int, String)] = blackStream.join(whiteStream)
    .where(_._1) //指定第一个Stream的关联Key
    .equalTo(_._1) //指定第二个Stream的关联Key
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(5)))
    .apply((black, white) => (black._1, black._2 + ":" + white._2))

  windowStream.print("Session Window Join")

  env.execute("Session Window Join Demo Job")
}
