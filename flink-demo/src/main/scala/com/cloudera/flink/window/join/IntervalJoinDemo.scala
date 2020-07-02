package com.cloudera.flink.window.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Interval Join  间接关联
 */
object IntervalJoinDemo extends App {


  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  //创建黑色元素数据集
  val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 20L), (4, 1L), (5, 4L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.milliseconds(1)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })

  //创建白色元素数据集
  val whiteStream: DataStream[(Int, Long)] = env.fromElements((2, 17L), (2, 18L), (2, 19L), (2, 20L), (2, 21L), (2, 22L), (2, 23L), (5, 3L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.milliseconds(1)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })

  //通过Join方法将两个数据集进行关联; 18 - 21 能关联上
  val windowStream: DataStream[(Int, String)] = blackStream.keyBy(_._1)
    .intervalJoin(whiteStream.keyBy(_._1))
    .between(Time.milliseconds(-2), Time.milliseconds(1))
    .process(new ProcessJoinFunction[(Int, Long), (Int, Long), (Int, String)]() {
      override def processElement(left: (Int, Long),
                                  right: (Int, Long),
                                  ctx: ProcessJoinFunction[(Int, Long), (Int, Long), (Int, String)]#Context,
                                  out: Collector[(Int, String)]): Unit = {
        out.collect(left._1, left._2 + ":" + right._2)
      }
    })

  windowStream.print("Interval Join")

  env.execute("Interval Join Demo Job")
}
