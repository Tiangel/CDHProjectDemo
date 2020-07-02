package com.cloudera.flink.watermark

import com.cloudera.flink.source.SensorReading
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class MyPeriodicWatermarks extends AssignerWithPeriodicWatermarks[SensorReading] {

  // 设定 1 秒延时，表示 1 秒以内的数据延时有效，超过 1 秒的数据被认为是迟到事件
  val maxOutOfOrderness = 1000l
  var currentMaxTimestamp: Long = _

  // 生成 watermark
  override def getCurrentWatermark: Watermark = {
    // 根据最大事件时间减去最大的乱序延时时间，然后得到 Watermark
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    // 获取当前时间时间
    val currentTimestamp: Long = element.timestamp
    // 对比当前的时间时间和历史最大时间时间，将最新的时间复制给 currentMaxTimestamp
    currentMaxTimestamp = currentMaxTimestamp.max(currentTimestamp)
    currentTimestamp
  }
}