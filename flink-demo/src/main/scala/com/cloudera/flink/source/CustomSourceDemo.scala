package com.cloudera.flink.source

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * 自定义 source
 */
object CustomSourceDemo {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = streamEnv.addSource(new CustomSource)

    // SplitStream:
    // 根据某些特征把一个 DataStream 拆分成两个或多个 DataStream
    val splitStream = stream.split(sensorData => {
      if (sensorData.temperature > 60) Seq("high") else Seq("low")
    })

    // 从一个 SplitStream 获取一个或多个 DataStream
    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high", "low")

    val warning: DataStream[(String, Double)] = high.map(sensorData => {
      (sensorData.id, sensorData.temperature)
    })
    // DataStream,DataStream -> ConnectedStreams:
    // 连接两个数据流，两个数据流被 Connect 之后，只是被放在了一个同一个流中;
    // 内部依然保持各自的数据和形式不发生任何变化，两个流相互独立
    val connected: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)


    // CoMap 和 CoFlatMap
    // ConnectedStreams -> DataStream
    // 作用于 ConnectedStreams 上，功能与 map 和 flatMap 一样，
    // 对 ConnectedStreams 中的每一个 Stream 分别进行 map 和 flatMap 处理
    val coMap: DataStream[Product] = connected.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    // 或者使用 CoMapFunction
//    val coMap1: DataStream[Product] = connected.map(new CoMapFunction[(String, Double), Product] {
//      override def map1(value: (String, Double)): Product = {
//        (value._1, value._2, "warning")
//      }
//
//      override def map2(value: SensorReading): Product = {
//        (value.id, "healthy")
//      }
//    })

    // connect 与 union 的区别:
    // Union 连接的流的类型必须是一致的，Connect 连接的两个流的类型可以不一致，可以在之后的 CoMap 中去调整为一致的。
    // Connect 只能操作两个流，Union 可以操作多个流
    val unionStream: DataStream[SensorReading] = high.union(low)

    unionStream.print()
    streamEnv.execute("Custom Source Demo")
  }
}





