package com.cloudera.flink.function.customfunction

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 求取平均值的聚合运算
 */
class MyAverageAggregateFunction extends AggregateFunction[(String, Long), (Long, Long), Double] {
  // 定义 createAccumulator 为两个参数的元组
  override def createAccumulator(): (Long, Long) = (0L, 0L)

  // 定义输入数据累加到 accumulator 的逻辑
  override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) = {
    (accumulator._1 + value._2, accumulator._2 + 1L)
  }

  // 根据累加器得出结果
  override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2

  // 定义累加器合并的逻辑
  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
}
