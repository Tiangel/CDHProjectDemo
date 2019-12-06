package com.cloudera

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Test {
  def sss(spark: SparkSession, users: RDD[String], ratings: RDD[String], movies: RDD[String]): Unit = {
    println("功能一: 通过 DataFrame 实现某部电影观看者中男性和女性不同年龄人数")
    // 首先把 Users 的数据格式化，即在 RDD 的基础上增加数据的元数据信息
    val schemaForUsers = StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::")
      .map(column => StructField(column, StringType, true)))
    // 然后把我们的每 条数据变成以 Row 为单位的数据
    val usersRDDRows = users.map(_.split("::")).
      map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    // 使用 SparkSession createDataFrame 方法，结合 Row StructType 的元数据信息
    // 基于 RDD 创建 DataFrame ，这时 RDD 就有了元数据信息的描述
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    // 也可以对 StructType 调用 add 方法来对不同的 StructField 赋予不同的类型
    val schemaforratings = StructType("UserID::MovieID".split("::")
      .map(column => StructField(column, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)


    val ratingsRDDRows = ratings.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame = spark.createDataFrame(ratingsRDDRows, schemaforratings)

    // 接着构建 movies 的 DataFrame
    val schemaformovies = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column, StringType, true)))

    val moviesRDDRows = movies.map(_.split(": :"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim))

    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)

    // 这里能够直接通过列名 MovieID 为 1193 过滤出这部电影，这些列名都是在上面指定的
    ratingsDataFrame.filter(s"MovieID = 1193 ")
      // Join 才候直接指定基于 UserID 进行 Join, 这相对于原生的 RDD 操作而言更加方便快捷:
      .join(usersDataFrame, "User")
      // 直接通过元数据信息中的 Gender 和 Age 进行数据的筛选
      .select("Gender", "Age")
      // 直接通过元数据信息中的 Gender 和 Age 进行数据的 groupBy 操作
      .groupBy("Gender", "Age")
      // 基于 groupBy 分组信息进行 count 统计操作， 并显示分组统计后的前 10 条信息
      .count().show(10)
  }

}
