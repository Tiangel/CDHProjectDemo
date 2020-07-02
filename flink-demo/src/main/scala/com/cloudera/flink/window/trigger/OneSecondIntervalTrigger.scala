package com.cloudera.flink.window.trigger

import com.cloudera.flink.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/** 可提前触发的触发器。触发周期不小于 1 秒 */
class OneSecondIntervalTrigger extends Trigger[SensorReading, TimeWindow] {

  override def onElement(element: SensorReading,
                         timestamp: Long,
                         window: TimeWindow,
                         ctx: Trigger.TriggerContext
                        ): TriggerResult = {
    // 如果之前没有设置过值 firstSeen 为 false
    val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))

    // 仅为第一个元素注册初始计数器
    if (!firstSeen.value()) {
      // 将水位线上取整到秒来计算下一次触发
      val t: Long = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      ctx.registerEventTimeTimer(t)
      // 为窗口结束时间注册计时器
      ctx.registerEventTimeTimer(window.getEnd)
      firstSeen.update(true)
    }
    // 继续。不会针对每个元素都计算
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    // 继续，不使用处理时间计时器
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.getEnd) {
      // 进行最终计算并清除窗口状态
      TriggerResult.FIRE_AND_PURGE
    } else {
      // 注册下一个用于提前触发的计时器
      val t: Long = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
      if (t < window.getEnd) {
        ctx.registerEventTimeTimer(t)
      }
      // 触发进行窗口计算
      TriggerResult.FIRE
    }
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // 清理触发器状态
    val firstSeen: ValueState[Boolean] = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("firstSeen", classOf[Boolean]))
    firstSeen.clear()
  }
}
