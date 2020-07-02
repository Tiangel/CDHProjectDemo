package com.cloudera.flink.sink

import com.cloudera.flink.sink.customsink.{MySqlSink, Student}
import org.apache.flink.streaming.api.scala._


object MysqlSinkdemo extends App {

  val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val host: String = "bigdata-dev-kafka-01"
  val port: Int = 7777


  val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)


  val ds: DataStream[Student] = socketDs.map(item => {
    val array: Array[String] = item.split(",")
    new Student(BigInt(array(0)), array(1), array(2), array(3).toInt)
  })

  ds.addSink(new MySqlSink)

  streamEnv.execute("MySQL Sink Demo")


}
