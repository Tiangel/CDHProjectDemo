package com.cloudera.flink.window.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 * Tumbling Window Join 滚动窗口关联
 */
object TumblingWindowJoinDemo extends App {


  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  //创建黑色元素数据集
  val blackStream: DataStream[(Int, Long)] = env.fromElements((2, 20L), (4, 1L), (5, 22L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.milliseconds(5)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })

  //创建白色元素数据集
  val whiteStream: DataStream[(Int, Long)] = env.fromElements((2, 17L), (2, 18L), (2, 19L), (2, 20L), (2, 21L), (2, 22L), (2, 23L), (5, 23L))
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, Long)](Time.milliseconds(5)) {
      override def extractTimestamp(element: (Int, Long)): Long = element._2
    })

  //通过Join方法将两个数据集进行关联
  val windowStream: DataStream[(Int, String)] = blackStream.join(whiteStream)
    .where(_._1) //指定第一个Stream的关联Key
    .equalTo(_._1) //指定第二个Stream的关联Key
    .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
    .apply((black, white) => (black._1, black._2 + ":" + white._2))


  windowStream.print("Tumbling Window Join")

  env.execute("Tumbling Window Join Job")
}
