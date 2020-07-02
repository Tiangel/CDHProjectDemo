package com.cloudera.flink.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.util.Random

class CustomSensorSource extends RichSourceFunction[SensorReading] {
  private var isRunning: Boolean = true

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()
    var curTemp = 1.to(10).map(i => ("sensor_" + i, 65 + rand.nextGaussian() * 20))
    while (isRunning) {
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
    isRunning = false
  }
}


