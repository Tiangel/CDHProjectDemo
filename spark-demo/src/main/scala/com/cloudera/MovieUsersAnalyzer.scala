package com.cloudera


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MovieUsersAnalyzer {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //数据存放的目录；
    var dataPath = "file:\\\\\\E:\\CDHProjectDemo\\spark-demo\\data\\moviedata\\medium\\"

    val conf = new SparkConf().setIfMissing("spark.master", "local[4]").setAppName("RDD Movie Users Analyzer")

    /**
      * Spark 2.0 引入 SparkSession 封装了 SparkContext 和 SQLContext ，并且会在
      * builder 的 getOrCreate 方法中判断是否有符合要求的 SparkSession 存在，有则使用，
      * 没有则进行创建
      */
    val spark = SparkSession.builder.config(conf).getOrCreate()
    //获取 SparkSession 的 SparkContext
    val sc = spark.sparkContext

    //把 Spark 程序运行时的日志设置为 warn 级别，以方便查看运行结果
    sc.setLogLevel("WARN")

    // 把用到的数据加栽进来转换为 RDD，此时使用 sc.textFile 并不会读取文件 ，
    // 而是标记了有这个操作，遇到 Action 级别算子时才会真正去读取文件
    // 数据格式 UserID::Gender::Age::Occupation::Zip-code
    // 用户ID::性别(F,M分别表示女性、男性)::年龄(使用7个年龄段标记)::职业::邮编。
    val users = sc.textFile(dataPath + "users.dat")
    // 数据格式 MovieID::Title::Geres
    // 电影::电影名称::电影类型
    val movies = sc.textFile(dataPath + "movies.dat")
    // 数据格式 UserID::MovieID::Rating::Timestamp
    // 用户ID::电影ID::评分(满分是5分)::时间戳
    val ratings: RDD[String] = sc.textFile(dataPath + "ratings.dat")


    getMoiveTop10(users, movies, ratings)

    //最后关闭 SparkSession
    spark.stop
  }

  /**
    * 功能一 :
    * 电影中平均得分 Top10（口碑最好）的电影
    */
  def getMoiveTop10(users: RDD[String], movies: RDD[String], ratings: RDD[String]): Unit = {
    println("所有电影中平均得分最高（口碑最好）的电影: ")

    /**
      * 第一步：从 ratingsRDD 中取出 MovieID 和 rating ，从 moviesRDD 中取出 MovieID 和 Name,
      * 如果后面的代码重复使用这些数据，则可以把它们缓存起来。首先把使用 map 算子 RDD 中的每个元素（即文件中的每一行）以 “::” 为分隔符进行拆分，
      * 然后使用 map 算子从拆分后得到的数组中取出需要用到的元素，井把得到的 RDD 缓存起来
      */
    val movieInfoRDD: RDD[(String, (String, String))] = movies.map(_.split("::")).map(x => (x(0), (x(1), x(2)))).cache()

    val ratingsRDD: RDD[(String, String, String, String)] = ratings.map(_.split("::")).map(x => (x(0), x(1), x(2), x(3))).cache()

    /**
      * 第二步：从 ratings 的数据中使用 map 算子获取到形如 (movieID,(ratings,1)) 格式的 RDD
      * 然后使用 reduceByKey 把每个电影的总评分以及点评人数算出来
      * 返回 (movieID,(Sum(ratings),Count(ratings))) 格式的 RDD
      */
    val moviesAndRatingsRDD: RDD[(String, (Double, Int))] = ratingsRDD.map(x => (x._2, (x._3.toDouble, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    /**
      * 第三步：把每个电影的 Sum(ratings) 和 Count(ratings) 相除，得到包含了电影 ID 和平均评分的 RDD
      */
    val avgRatingsRDD: RDD[(String, Double)] = moviesAndRatingsRDD.map(x => (x._1, x._2._1.toDouble / x._2._2))

    /**
      * 第四步：把 avgRatingsRDD 与 movieInfoRDD 通过关键宇 (key) 连接到一起，得到形如 (movieID,(MovieName, AvgRating))的 RDD ,
      * 然后格式化为 (AvgRating,MovieName)，并按照 key （也就是平均评分）降序排列，最终取出前 10 个并打印出来.
      */
    avgRatingsRDD.join(movieInfoRDD)
      .map(item => (item._2._1, item._2._2._1))
      .sortByKey(false)
      .take(10)
      .foreach(record => println(record._2 + "评分为: " + record._1))
  }


  /**
    * 功能二:
    * 分析最受男性喜爱的电影 Top10 和最受女性喜爱的电影 Top10
    */
  def genderMovieTop10(users: RDD[String], movies: RDD[String], ratings: RDD[String]): Unit = {

    // (用户ID,性别(F,M分别表示女性、男性),年龄(使用7个年龄段标记),职业,邮编)
    //    用户ID::电影ID::评分(满分是5分)::时间戳
    val userGenderRDD: RDD[(String, String, String, String, String)] = users.map(_.split("::")).map(x => (x(0), x(1), x(2), x(3), x(4)))

    val genderRatingsCacheRDD: RDD[(String, ((String, String, String), String))] = ratings.map(_.split("::"))
      .map(x => (x(0), x(1), x(2), x(3)))
    //男性
    //    userGenderRDD.filter(x => x._2.equals("F")).
    //    genderRatingsCacheRDD.take(10).foreach(println(_))
    //
    //    val maleFilteredRatingsRDD: RDD[(String, String, String)] =
    //      genderRatingsCacheRDD.filter(x => x._2._2.equals("M")).map(x => x._2._1)
    //
    //    val femaleFilteredRatingsRDD: RDD[(String, String, String)] =
    //      genderRatingsCacheRDD.filter(x => x._2._2.equals("F")).map(x => x._2._1)
    //
    //
    //    println("所有电影中最受男性喜爱的电影 Top10 : ")
    //    maleFilteredRatingsRDD.map(x => (x._1, (x._3.toDouble, 1)))
    //      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    //      .map(x => (x._1, x._2._1.toDouble / x._2._2))
    //      .join(movieinfo)
    //      .map(item => (item._2._1, item._2._2))
    //      .sortByKey(false)
    //      .take(10)
    //      .foreach(record => println(record._2 + "评分为: " + record._1 ))
    //
    //    println("所有电影中最受女性喜爱的电影 Top10 : ")
    //    maleFilteredRatingsRDD.map(x => (x._1, (x._3.toDouble, 1)))
    //      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    //      .map(x => (x._1, x._2._1.toDouble / x._2._2))
    //      .join(movieinfo)
    //      .map(item => (item._2._1, item._2._2))
    //      .sortByKey(false)
    //      .take(10)
    //      .foreach(record => println(record._2 + "评分为: " + record._1 ))
  }

}
