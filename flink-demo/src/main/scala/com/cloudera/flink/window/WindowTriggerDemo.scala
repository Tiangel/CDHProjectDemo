package com.cloudera.flink.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTriggerDemo extends App {

  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  streamEnv.setParallelism(1)

//  val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)
//
//  val inputStream: DataStream[(String, Long, Long)] = socketDs.map(x => {
//    val arr: Array[String] = x.split("\\W+", -1)
//    (arr(0), arr(1).toLong, arr(2).toLong)
//  })

  val inputStream: DataStream[(String, Long, Long)] = streamEnv.fromElements(
    ("flink", 1L, 1000000050000L),
    ("flink", 3L, 1000000054000L),
    ("flink", 5L, 1000000079900L),
    ("flink", 7L, 1000000115000L),
    ("kafka", 2L, 1000000095000L),
    ("kafka", 4L, 1000000108000L)
  )

  val delay = 5L
  val windowGap = 10L

  val windowStream: DataStream[(String, Long, Long)] = inputStream
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.seconds(delay)) {
      override def extractTimestamp(element: (String, Long, Long)): Long = {
        println(element._1 + "\t" + element._2 + " watermark -> " + getCurrentWatermark().getTimestamp + " event_time -> " + element._3)
        element._3
      }
    })
    .keyBy(0)
    //指定窗口类型
    .window(EventTimeSessionWindows.withGap(Time.seconds(windowGap)))
    //指定聚合函数逻辑，将根据ID将第二个字段求和
    .trigger(ContinuousEventTimeTrigger.of(Time.seconds(windowGap)))
    .reduce(new ReduceFunction[(String, Long, Long)] {
      override def reduce(t1: (String, Long, Long), t2: (String, Long, Long)): (String, Long, Long) = {
        (t1._1, t1._2 + t2._2, if (t1._3 <= t2._3) t2._3 else t1._3)
      }
    })

  windowStream.print()
  streamEnv.execute("Window Trigger Demo Execute")
}
