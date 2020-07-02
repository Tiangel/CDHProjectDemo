package com.cloudera.flink.watermark


import com.cloudera.flink.source.{CustomSensorSource, SensorReading}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 周期性的生成 watermark：系统会周期性的将 watermark 插入到流中(水位线也是一种特殊的事件!)。
 * 默认周期是 200 毫秒。
 */
object PeriodicWatermarksDemo extends App {

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // 每隔 5 秒产生一个 watermark
  streamEnv.getConfig.setAutoWatermarkInterval(5000)

  val dataStream: DataStream[SensorReading] = streamEnv.addSource(new CustomSensorSource)


  //  val watermarksStream: DataStream[SensorReading] = dataStream.assignTimestampsAndWatermarks(
  //    new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
  //      override def extractTimestamp(element: SensorReading): Long = element.timestamp
  //    })

  val watermarksStream: DataStream[SensorReading] = dataStream.assignTimestampsAndWatermarks(new MyPeriodicWatermarks())

  watermarksStream.map(data => (data.id, data.temperature))
    .keyBy(0)
    .timeWindow(Time.seconds(10), Time.seconds(3))
    // 统计10秒内的最低温度值
    .reduce((result, data) => (data._1, result._2.min(data._2)))
    .print()


  streamEnv.execute("Periodic Watermarks Demo Execute")
}
