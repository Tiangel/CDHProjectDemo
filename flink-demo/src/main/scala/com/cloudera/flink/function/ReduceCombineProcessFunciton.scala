package com.cloudera.flink.function

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 *
 */
object ReduceCombineProcessFunciton extends App {

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val inputStream = streamEnv.fromCollection(
    List(("a", 3L, 1), ("a", 1L, 3), ("b", 1L, 1), ("b", 3L, 2))
  ).assignAscendingTimestamps(t => t._2)


  val result: DataStream[(Long, (String, Long, Int))] = inputStream.keyBy(_._1)
    .timeWindow(Time.seconds(10))
    .reduce(
      //定义ReduceFunction，完成求取最小值的逻辑
      (r1: (String, Long, Int), r2: (String, Long, Int)) => {
        if (r1._3 > r2._3) {
          r2
        } else {
          r1
        }
      },
      //定义ProcessWindowsFunciton，完成对窗口元数据的采集
      (key: String, window: TimeWindow, minReadings: Iterable[(String, Long, Int)], out: Collector[(Long, (String, Long, Int))]) => {
        val min = minReadings.iterator.next()
        //采集窗口结束时间和最小值对应的数据元素
        out.collect((window.getEnd, min))
      })

  result.print()

  streamEnv.execute("Reduce Combine Process Function Demo")
}
