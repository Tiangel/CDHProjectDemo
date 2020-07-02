package com.cloudera.flink.function

import com.cloudera.flink.function.customfunction.CustomFilter
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object CustomFilterDemo {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val host: String = "bigdata-dev-kafka-01"
    val port: Int = 7777


    val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)

    // Flink 暴露了所有 udf 函数的接口(实现方式为接口或者抽象类)。MapFunction, FilterFunction, ProcessFunction 等等。
    // 使用 UDF 函数
    val filterDs1: DataStream[String] = socketDs.filter(new CustomFilter("flink"))

    // 使用匿名类
    val filterDs2: DataStream[String] = socketDs.filter(
      // "富函数" 是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都有其 Rich 版本。
      // 它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一些生命周期方法，所以可以实现更复杂的功能。
      new RichFilterFunction[String] {
        override def filter(value: String): Boolean = {
          value.contains("flink")
        }
      })

    val anonFilter: DataStream[String] = socketDs.filter(_.contains("flink"))

    filterDs2.print()
    streamEnv.execute("Filter function demo")
  }
}
