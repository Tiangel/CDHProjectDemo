package com.cloudera

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CustomSort {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[4]")
      .setAppName("CustomSort")

    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    //获取 SparkSession 的 SparkContext
    val sc: SparkContext = spark.sparkContext

    // 姓名 年龄 收入
    // 排序规则，先按收入降序，相等再按年龄升序
    val users = Array("张三 28 20000", "李四 26 23000", "王五 28 25000", "赵六 30 30000")
    // 将数据并行化变成 RDD
    val lines: RDD[String] = sc.parallelize(users)
    val sortedResult1 = sort1(lines)
    val sortedResult2 = sort2(lines)

    sortedResult1.take(10).foreach(println)
    println("=====================================")
    sortedResult2.take(10).foreach(println)
  }

  def sort1(lines: RDD[String]): RDD[String] = {
    // 切分整理数据，将需要的数据切分出来，放到一个类中，
    // 这个类继承 ordered，然后重写compare，这个类在调用相应的排序方法的时候就会执行重写后的排序规则
    val pairWithSortkey = lines.map(line => {
      val splited: Array[String] = line.split(" ")
      (new SecondarySortKey(splited(1).toDouble, splited(2).toDouble), line)
    })
    // 传入排序规则，调用这个类重写的排序规则
    val sorted: RDD[(SecondarySortKey, String)] = pairWithSortkey.sortByKey(true)
    val sortedResult: RDD[String] = sorted.map(sortedline => sortedline._2)
    sortedResult
  }

  def sort2(lines: RDD[String]): RDD[(String, String, String)] = {

    val pairWithSortkey: RDD[(String, String, String)] = lines.map(line => {
      val splited: Array[String] = line.split(" ")
      (splited(0),splited(1), splited(2))
    })
    // 传入一个排序规则，不改变数据格式，只改变顺序
    val sortedResult: RDD[(String, String, String)] = pairWithSortkey.sortBy(s => new SecondarySortKey(s._2.toDouble, s._3.toDouble))
    sortedResult
  }

  class SecondarySortKey(val first: Double, val second: Double) extends Ordered[SecondarySortKey] with Serializable {
    override def compare(that: SecondarySortKey): Int = {
      // 既然是二次排序，那么首先要判断第一个排序字段是否相等，如果不相等，就直接排序
      if (this.first - that.first != 0) {
        (this.first - that.first).toInt
      } else {
        //如果第一个字段相等，则比较第二个字段，若想实现多次排序，也可以按照这个模式继
        //续比较下去
        if (this.second - that.second > 0) {
          Math.ceil(this.second - that.second).toInt
        } else if (this.second - that.second < 0) {
          Math.floor(this.second - that.second).toInt
        } else {
          (this.second - that.second).toInt
        }
      }
    }
  }
}
