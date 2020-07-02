package com.cloudera.flink.function


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WindowsProcessFunctionDemo extends App {

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val host: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)

  val processWindowStream: DataStream[(String, Long, Long, Long, Long, Long)] = socketDs.map(x => {
    val arr: Array[String] = x.split("\\W+", -1)
    (arr(0), arr(1).toLong, arr(2).toLong)
  }).assignAscendingTimestamps(_._3)
    .keyBy(_._1)
    .timeWindow(Time.seconds(10))
    .process(new ProcessWindowFunction[(String, Long, Long), (String, Long, Long, Long, Long, Long), String, TimeWindow] {
      override def process(key: String,
                           context: Context,
                           elements: Iterable[(String, Long, Long)],
                           out: Collector[(String, Long, Long, Long, Long, Long)]
                          ): Unit = {
        //定义求和，最大值,最小值，平均值，窗口时间逻辑
        val sum = elements.map(_._2).sum
        val min = elements.map(_._2).min
        val max = elements.map(_._2).max
        var avg = sum / elements.size
        val windowEnd = context.window.getEnd
        //通过out.collect返回计算好的结果
        out.collect((key, min, max, sum, avg, windowEnd))
      }
    })

  processWindowStream.print()
  streamEnv.execute("Windows Process Function Demo")

}
