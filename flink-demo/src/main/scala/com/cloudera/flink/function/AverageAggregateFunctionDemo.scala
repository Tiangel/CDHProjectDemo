package com.cloudera.flink.function


import com.cloudera.flink.function.customfunction.MyAverageAggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object AverageAggregateFunctionDemo extends App {


  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val host: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)

  socketDs.map(_.split("\\W+", -1))
    .map(x => (x(0), x(1)))
    .keyBy(0)
    .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .aggregate(new MyAverageAggregateFunction)

  streamEnv.execute("Average Aggregate Function Demo")

}
