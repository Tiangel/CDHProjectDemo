package com.cloudera

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer

/**
  * 自定义累加器
  * 实现把字符串合并为数组的累加器：
  * 首先要继承 AccurnulatorV ，并指定输入为 String 类型，输出为 ArrayBuffer[String]
  */
class MyAccurnulator extends AccumulatorV2[String, ArrayBuffer[String]] {

  // 设置累加器的结果，类型为 ArrayBuffer[String]
  private var result = ArrayBuffer[String]()

  // 判断累加器当前值是否为 "零值"，这里我们指定如果 result 的 size 为 0，则累加器的当前值是 "零值"
  override def isZero: Boolean = this.result.size == 0

  // copy 方法设置为新建本累加器，并把 result 赋给新的累加器
  override def copy(): AccumulatorV2[String, ArrayBuffer[String]] = {
    val newAccm: MyAccurnulator = new MyAccurnulator
    newAccm.result = this.result
    newAccm
  }

  // reset 方法设置为把 result 设置为新的 ArrayBuffer
  override def reset(): Unit = {
    this.result = new ArrayBuffer[String]()
  }

  // add 方法把传进来的字符串添加到 result
  override def add(v: String): Unit = this.result += v

  // merge 方法把两个累加器的 result 合并起来
  override def merge(other: AccumulatorV2[String, ArrayBuffer[String]]): Unit = {
    result.++=:(other.value)
  }

  // value 方法返回 result
  override def value: ArrayBuffer[String] = this.result
}



