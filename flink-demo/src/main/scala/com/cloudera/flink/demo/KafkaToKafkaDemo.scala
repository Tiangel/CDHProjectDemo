package com.cloudera.flink.java.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.extensions._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala._

object KafkaToKafkaDemo {

  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val GROUP_ID = "group.id"
  val RETRIES = "retries"
  val TOPICS = "topics"

  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    // 设置时间模式为时间时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // checkpoint 设置





  }
}
