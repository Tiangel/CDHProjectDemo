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
      val arr: Array[String] = x.split(" ")
      (arr(0), arr(1).toLong)
    })

  inputStream.keyBy(_._1)
    //指定输入参数类型和状态参数类型
    .mapWithState[(String, Long), Long]((in: (String, Long), count: Option[Long]) => {
      // 判断 count 类型是否非空
      count match {
        // 输出 key,count，并在原来的 count 数据上累加
        case Some(c) => ((in._1, in._2 + c), Some(in._2 + c))
        // 如果输入状态为空，则将指标填入
        case None => ((in._1, in._2), Some(in._2))
      }
    }).print()

  env.execute("Map With State Demo Job")
}
