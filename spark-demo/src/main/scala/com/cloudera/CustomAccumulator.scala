package com.cloudera

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{AccumulatorParam, SparkConf, SparkContext}

object CustomAccumulator {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setIfMissing("spark.master", "local[4]").setAppName("CustomAccumulator")

    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    //获取 SparkSession 的 SparkContext
    val sc: SparkContext = spark.sparkContext


    // 定义累加器，这里直接使用 SparkContext 内置的累加器，设置初始值为 ，名字为 "My Accumulator"
    val accum1 = sc.accumulator(0, "My Accumulator") // 计算值
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum1 += x) // 获取累加器的值，（ Executor 上面只能对累加器进行累加操作，只有 Driver 才能读取累加器的值，
    // Driver 读取值的时候会把各个 Executor 上存储的本地累加器的值加起来），
    // 这里的结果是 10
    println(accum1.value)


    // 与 Spark 不同的是，需要指定累加器的类型，
    // 目前 SparkContext Long 类型和 Double 类型的累加器可以直接使用（不需要指定初始值）
    val accum2 = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum2.add(x))
    println(accum2.value)


    val myAccum = new MyAccurnulator()
    // SparkContext 注册累加器
    sc.register(myAccum)

    // 把 "a", "b", "c", "d" 添加进累加器的 result 数组并打印出
    sc.parallelize(Array("a", "b", "c", "d" )).foreach(x => myAccum.add(x))
    println(myAccum.value)
  }
}
