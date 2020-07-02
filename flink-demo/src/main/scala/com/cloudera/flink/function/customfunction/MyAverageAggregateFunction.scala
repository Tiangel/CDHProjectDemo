package com.cloudera.flink.function.customfunction

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 求取平均值的聚合运算
 */
class MyAverageAggregateFunction extends AggregateFunction[(String, Long), (String, Long, Long), (String, Double)] {
  // 定义 createAccumulator 为两个参数的元组
  override def createAccumulator(): (String, Long, Long) = ("", 0L, 0L)

  // 定义输入数据累加到 accumulator 的逻辑
  override def add(value: (String, Long), accumulator: (String, Long, Long)): (String, Long, Long) = {
    (value._1, accumulator._2 + value._2, accumulator._3 + 1L)
  }

  // 根据累加器得出结果
  override def getResult(accumulator: (String, Long, Long)): (String, Double) = (accumulator._1, accumulator._2 / accumulator._3)

  // 定义累加器合并的逻辑
  override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
    (a._1, a._2 + b._2, a._3 + b._3)
  }
}
