package com.cloudera.flink.function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * The implementation of the ProcessFunction that maintains the count and timeouts
 */
class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String, Long)] {

  /** The state that is maintained by this process function */
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext.getState(
    new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))

  override def processElement(value: (String, String),
                              ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#Context,
                              out: Collector[(String, Long)]): Unit = {
    // initialize or retrieve/update the state
    val current: CountWithTimestamp = state.value match {
      case null => {
        CountWithTimestamp(value._1, 1, ctx.timestamp)
      }
      case CountWithTimestamp(key, count, lastModified) => {
        CountWithTimestamp(key, count + 1, ctx.timestamp)
      }
    }
    // write the state back
    state.update(current)
    // schedule the next timer 60 seconds from the current event time
    ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#OnTimerContext,
                       out: Collector[(String, Long)]): Unit = {
    state.value match {
      case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 60000) => {
        out.collect((key, count))
      }
      case _ =>
    }
  }
}


/**
 * The data type stored in the state
 */
case class CountWithTimestamp(key: String, count: Long, lastModified: Long)
