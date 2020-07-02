package com.cloudera.flink.state

import org.apache.flink.streaming.api.scala._

object MapWithStateDemo extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  //创建元素数据集
  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val inputStream: DataStream[(String, Long)] = env.socketTextStream(hostname, port)
    .map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(0), strings(1).toLong)
    })

  inputStream.keyBy(_._1)
    //指定输入参数类型和状态参数类型
    .mapWithState((in: (String, Long), count: Option[String]) => {
      // 判断 count 类型是否非空
      count match {
        // 输出 key,count，并在原来的 count 数据上累加
        case Some(c) => ((in._1, c), Some(c + in._2))
        // 如果输入状态为空，则将指标填入
        case None => ((in._1, 0), Some(in._2))
      }
    }).print()

  env.execute("Map With State Demo Job")
}
