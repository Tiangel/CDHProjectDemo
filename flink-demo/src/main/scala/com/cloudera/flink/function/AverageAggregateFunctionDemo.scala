package com.cloudera.flink.function


import com.cloudera.flink.function.customfunction.MyAverageAggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object AverageAggregateFunctionDemo extends App {


  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val host: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)

  val aggregateWindowStream: DataStream[(String, Double)] = socketDs.map(x => {
    val arr: Array[String] = x.split("\\W+", -1)
    (arr(0), arr(1).toLong)
  }).keyBy(0)
    //指定窗口类型
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    //指定聚合函数逻辑，将第二个字段求平均值
    .aggregate(new MyAverageAggregateFunction)

  aggregateWindowStream.print()
  streamEnv.execute("Average Aggregate Function Demo")

}
