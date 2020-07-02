package com.cloudera.flink.state

import com.cloudera.flink.source.SensorReading
import org.apache.flink.streaming.api.scala._

object FlatMapWithStateDemo extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  //创建元素数据集
  val hostname: String = "bigdata-dev-kafka-01"
  val port: Int = 7777

  val inputStream: DataStream[(String, Double)] = env.socketTextStream(hostname, port)
    .map(x => {
      val arr: Array[String] = x.split(" ")
      (arr(0), arr(1).toDouble)
    })

  /*
  *  判断相邻两条数据值是否超过 1.7
  */
  inputStream.keyBy(_._1)
    .flatMapWithState[(String, Double, Double), Double]((in: (String, Double), lastTemp: Option[Double]) => {
      lastTemp match {
        //之前的温度还未定义，只需要更新前一个温度值
        case None => (List.empty, Some(in._2))
        case Some(tmp) => {
          // 比较温差和阈值
          val tempDiff: Double = (in._2 - tmp).abs
          if (tempDiff > 1.7) {
            // 超过阈值，发出警报并更新前一个温度值
            (List((in._1, in._2, tempDiff)), Some(in._2))
          } else {
            // 没有超出阈值，仅更新前一个温度值
            (List.empty, Some(in._2))
          }
        }
      }
    }).print()


  /*inputStream.keyBy(_._1)
    .flatMapWithState[(String, Double, Double), Double]({
      //之前的温度还未定义，只需要更新前一个温度值
      case (in: (String, Double), None) => (List.empty, Some(in._2))
      case (r: (String, Double), lastTemp: Some[Double]) => {
        // 比较温差和阈值
        val tempDiff: Double = (r._2 - lastTemp.get).abs
        if (tempDiff > 1.7) {
          // 超过阈值，发出警报并更新前一个温度值
          (List((r._1, r._2, tempDiff)), Some(r._2))
        } else {
          // 没有超出阈值，仅更新前一个温度值
          (List.empty, Some(r._2))
        }
      }
    }).print()*/

  env.execute("Map With State Demo Job")
}
