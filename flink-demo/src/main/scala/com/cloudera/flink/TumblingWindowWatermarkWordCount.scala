package com.cloudera.flink


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time


object TumblingWindowWatermarkWordCount {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
    /*
    Event Time：是事件创建的时间。它通常由事件中的时间戳描述，例如采集的。
    Ingestion Time：是数据进入 Flink 的时间。
    Processing Time：是每一个执行基于时间操作的算子的本地系统时间，与机器相关。
    默认的时间属性就是 Processing Time。
    */
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //这里为了便于理解，设置并行度为1,默认并行度是当前机器的cpu数量
    streamEnv.setParallelism(1)


    val host = "bigdata-dev-kafka-01"
    val port = 7777

    /*指定数据源 从socket的9000端口接收数据，先进行了不合法数据的过滤*/
    val sourceDS: DataStream[String] = streamEnv.socketTextStream(host, port)


    val wordCount: DataStream[(String, String, Int)] = sourceDS.filter(_ != null)
      .map(_.split(","))
      .filter(_.length == 2)
      .map(x => (x(0), x(1), 1))
//      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, String, Int)] {
//        var currentMaxTimestamp: Long = 0L
//        val maxOutOfOrderness = 5000L
//
//        override def getCurrentWatermark: Watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
//
//        override def extractTimestamp(t: (String, String, Int), l: Long): Long = {
//          currentMaxTimestamp = Math.max(t._1.toLong, currentMaxTimestamp)
//          t._1.toLong
//        }
//      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String,String,Int)](Time.milliseconds(5000)){
        override def extractTimestamp(t: (String, String, Int)): Long = {
          t._1.toLong
        }
      })
      .keyBy(1)
      .timeWindow(Time.seconds(30))
      .sum(2)

    wordCount.print("\n单词统计：");
    streamEnv.execute("Window WordCount");


  }

}
