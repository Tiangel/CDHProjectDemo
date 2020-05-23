package com.cloudera.flink.demo

import com.sun.org.apache.bcel.internal.generic.NEW
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object CustomSourceDemo {
  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val stream: DataStream[SensorReading] = streamEnv.addSource(new MyCustomSource())

    stream.print()
    streamEnv.execute("Custom Source Demo")
  }
}


// 定义样例类，传感器 id，时间戳，温度
case class SensorReading(id: String, timestamp: Long, temperature: Double)

class MyCustomSource extends SourceFunction[SensorReading] {
  // flag: 表示数据源是否还在正常运行
  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, 65 + rand.nextGaussian() * 20))
    while (running) {
      // 更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1, curTime, t._2))
      )
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}