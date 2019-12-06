package com.cloudera

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object MovieUsersAnalyzer {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //数据存放的目录；
    var dataPath = "file:\\\\\\E:\\CDHProjectDemo\\spark-demo\\data\\moviedata\\medium\\"

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[4]")
      .setAppName("RDD Movie Users Analyzer")

    /**
      * Spark 2.0 引入 SparkSession 封装了 SparkContext 和 SQLContext ，并且会在
      * builder 的 getOrCreate 方法中判断是否有符合要求的 SparkSession 存在，有则使用，
      * 没有则进行创建
      */
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    //获取 SparkSession 的 SparkContext
    val sc = spark.sparkContext

    //把 Spark 程序运行时的日志设置为 warn 级别，以方便查看运行结果
    sc.setLogLevel("WARN")

    // 把用到的数据加栽进来转换为 RDD，此时使用 sc.textFile 并不会读取文件 ，
    // 而是标记了有这个操作，遇到 Action 级别算子时才会真正去读取文件
    // 数据格式 UserID::Gender::Age::Occupation::Zip-code
    // 用户ID::性别(F,M分别表示女性、男性)::年龄(使用7个年龄段标记)::职业::邮编
    val users = sc.textFile(dataPath + "users.dat")
    // 数据格式 MovieID::Title::Genres
    // 电影::电影名称::电影类型
    val movies = sc.textFile(dataPath + "movies.dat")
    // 数据格式 UserID::MovieID::Rating::Timestamp
    // 用户ID::电影ID::评分(满分是5分)::时间戳
    val ratings: RDD[String] = sc.textFile(dataPath + "ratings.dat")

    //    println("所有电影中平均得分最高（口碑最好）的电影: ")
    //    getMoiveTop10(users, movies, ratings)
//    genderMovieTop10(users, movies, ratings)

//    println("对电影评分数据以 mes tamp Rating 两个维度进行二次降序排列 : ")
//    ratingsSorted(ratings);

    genderAndAgeCountMovieByDataFrame(spark, users, ratings, movies)
    genderAndAgeCountMovieByDataSet(spark, users, ratings, movies)
    //最后关闭 SparkSession
    spark.stop
  }

  /**
    * 功能一 :
    * 电影中平均得分 Top10（口碑最好）的电影
    */
  def getMoiveTop10(users: RDD[String], movies: RDD[String], ratings: RDD[String]): Unit = {
    /**
      * 第一步：从 ratingsRDD 中取出 MovieID 和 rating ，从 moviesRDD 中取出 MovieID 和 Name,
      * 如果后面的代码重复使用这些数据，则可以把它们缓存起来。首先把使用 map 算子 RDD 中的每个元素（即文件中的每一行）以 “::” 为分隔符进行拆分，
      * 然后使用 map 算子从拆分后得到的数组中取出需要用到的元素，井把得到的 RDD 缓存起来
      */
    val movieInfoRDD: RDD[(String, (String, String))] = movies.map(_.split("::"))
      .map(x => (x(0), (x(1), x(2)))).cache()

    val ratingsRDD: RDD[(String, String, String, String)] = ratings.map(_.split("::"))
      .map(x => (x(0), x(1), x(2), x(3))).cache()

    /**
      * 第二步：从 ratings 的数据中使用 map 算子获取到形如 (movieID,(ratings,1)) 格式的 RDD
      * 然后使用 reduceByKey 把每个电影的总评分以及点评人数算出来
      * 返回 (movieID,(Sum(ratings),Count(ratings))) 格式的 RDD
      */
    val moviesAndRatingsRDD: RDD[(String, (Double, Int))] = ratingsRDD.map(x => (x._2, (x._3.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

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
      .sortByKey(false).take(10)
      .foreach(record => println(record._2 + "评分为: " + record._1))
  }


  /**
    * 功能二:
    * 分析最受男性喜爱的电影 Top10 和最受女性喜爱的电影 Top10
    */
  def genderMovieTop10(users: RDD[String], movies: RDD[String], ratings: RDD[String]): Unit = {


    // (用户ID,(性别,年龄,职业,邮编))
    val userGenderRDD: RDD[(String, (String, String, String, String))] = users.map(_.split("::"))
      .map(x => (x(0), (x(1), x(2), x(3), x(4))))
    // (用户ID,((电影ID,评分,时间戳),(性别,年龄,职业,邮编)))
    val genderRatingsCacheRDD: RDD[(String, ((String, String, String), (String, String, String, String)))] =
      ratings.map(_.split("::"))
      .map(x => (x(0), (x(1), x(2), x(3)))).join(userGenderRDD).cache()

    // (电影,(电影名称,电影类型))
    val moviesInfoRDD: RDD[(String, (String, String))] = movies.map(_.split("::"))
      .map(x => (x(0), (x(1), x(2)))).cache()

    // 女性
    // (电影ID,(总评分,总人数))
    val femaleMovieTotalRDD: RDD[(String, (Double, Int))] = genderRatingsCacheRDD
      .filter(x => x._2._2._1.equals("F"))
      .map(x => (x._2._1._1, (x._2._1._2.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("所有电影中最受女性喜爱电影Top10: ")
    femaleMovieTotalRDD.map(x => (x._1, x._2._1 / x._2._2)) // (电影ID, 总评分/总人数)
      .join(moviesInfoRDD) // (电影ID, (平均评分,(电影名称,电影类型)))
      .map(x => (x._2._1, x._2._2._1)).sortByKey(false) // 倒序
      .take(10).foreach(record => println(record._2 + "评分为: " + record._1))


    // 男性
    // (电影ID,(总评分,总人数))
    val maleMovieTotalRDD: RDD[(String, (Double, Int))] = genderRatingsCacheRDD
      .filter(x => x._2._2._1.equals("M"))
      .map(x => (x._2._1._1, (x._2._1._2.toDouble, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    println("所有电影中最受男性喜爱电影Top10: ")
    maleMovieTotalRDD.map(x => (x._1, x._2._1.toDouble / x._2._2)) // (电影ID, 总评分/总人数)
      .join(moviesInfoRDD) // (电影ID, (平均评分,(电影名称,电影类型)))
      .map(x => (x._2._1, x._2._2._1)).sortByKey(false) // 倒序
      .take(10).foreach(record => println(record._2 + "评分为: " + record._1))
  }

  /**
    * 功能三:
    * 二次排序
    * 按照时间戳和评分排序
    */
  def ratingsSorted(ratinsRDD: RDD[String]): Unit = {

    val pairWithSortkey = ratinsRDD.map(line => {
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
    })
    // 直接调用 sortByKey ，此时会按照之前实现的 compare 方法排序
    val sorted = pairWithSortkey.sortByKey(false)

    val sortedResult = sorted.map(sortedline => sortedline._2)
    sortedResult.take(10).foreach(println)
  }


  /**
    * 功能四:
    * 通过 DataFrame 实现某部电影观看者中男性和女性不同年龄人数
    */
  def genderAndAgeCountMovieByDataFrame(spark: SparkSession, users: RDD[String], ratings: RDD[String], movies: RDD[String]): Unit = {

    println("功能一: 通过 DataFrame 实现某部电影观看者中男性和女性不同年龄人数 ")

    val schemaForUsers: StructType = StructType("UserID::Gender::Age::OccupationID::Zip-code".split("::")
      .map(column => StructField(column, StringType, true)))
    // 首先把 Users 的数据格式化，即在 RDD 的基础上增加数据的元数据信息

    // 数据格式 UserID::Gender::Age::Occupation::Zip-code
    // 用户ID::性别(F,M分别表示女性、男性)::年龄(使用7个年龄段标记)::职业::邮编
    // 然后把我们的每条数据变成以 Row 为单位的数据
    val usersRDDRows: RDD[Row] = users.map(_.split("::")).
      map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))

    // 使用 SparkSession createDataFrame 方法，结合 Row StructType 的元数据信息
    // 基于 RDD 创建 DataFrame ，这时 RDD 就有了元数据信息的描述
    // RDD 转换成 DataFrame
    val usersDataFrame: DataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    // 也可以对 StructType 调用 add 方法来对不同的 StructField 赋予不同的类型
    val schemaForRatings: StructType = StructType("UserID::MovieID".split("::")
      .map(column => StructField(column, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("Timestamp", StringType, true)


    // 数据格式 UserID::MovieID::Rating::Timestamp
    // 用户ID::电影ID::评分(满分是5分)::时间戳
    val ratingsRDDRows: RDD[Row] = ratings.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame: DataFrame = spark.createDataFrame(ratingsRDDRows, schemaForRatings)

    // DataFrame 转换为 DataSet
    import spark.implicits._
    println("DataFrame 转换为 DataSet :")
    val ratingsDataSet: Dataset[Rating] = ratingsDataFrame.as[Rating]
    ratingsDataSet.show()
    println("DataSet 转换为 DataFrame :")
    val frame: DataFrame = ratingsDataSet.toDF()
    frame.show()

    // 接着构建 movies 的 DataFrame
    val schemaForMovies: StructType = StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column, StringType, true)))

    // 数据格式 MovieID::Title::Genres
    // 电影::电影名称::电影类型
    val moviesRDDRows: RDD[Row] = movies.map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim))

    val moviesDataFrame: DataFrame = spark.createDataFrame(moviesRDDRows, schemaForMovies)


    // 这里能够直接通过列名 MovieID 为 1193 过滤出这部电影，这些列名都是在上面指定的
    ratingsDataFrame.filter(s"MovieID = 1193")
      // Join 时候直接指定基于 UserID 进行 Join, 这相对于原生的 RDD 操作而言更加方便快捷:
      .join(usersDataFrame, "UserID")
      // 直接通过元数据信息中的 Gender 和 Age 进行数据的筛选
      .select("Gender", "Age")
      // 直接通过元数据信息中的 Gender 和 Age 进行数据的 groupBy 操作
      .groupBy("Gender", "Age")
      // 基于 groupBy 分组信息进行 count 统计操作， 并显示分组统计后的前 10 条信息
      .count().show(10)
//      .foreach(println(_))

//    localTempView(ratingsDataFrame, usersDataFrame, spark)
    globalTempView(ratingsDataFrame, usersDataFrame, spark)

  }

  /**
    * 会话级别的临时表
    */
  def localTempView(ratingsDataFrame: DataFrame ,usersDataFrame: DataFrame,spark: SparkSession): Unit ={
    println("功能二: LocalTempView 实现某部电影观看荷中不同性别不同年龄分别有多少" )
    // 既然使用 SQL 语句，那么表肯定是要有的，所以需要先把 DataFrame 注册为临时表
    // createTempView 创建的临时表是会话级别的, 会话结束时这个表也会消失
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView ("users")

    // 然后写 SQL 语句, 直接使用 SparkSession 的 sql 方法执行 SQL 语句即可
    val sql_local =
      """
        |SELECT Gender,
        |       Age,
        |       count(*)
        |FROM users u
        |JOIN ratings AS r ON u.UserID = r.UserID
        |WHERE MovieID = 1193
        |GROUP BY Gender,
        |         Age
      """.stripMargin
    spark.sql(sql_local).show(10)
  }

  /**
    * Application 级别的临时表
    */
  def globalTempView(ratingsDataFrame: DataFrame ,usersDataFrame: DataFrame,spark: SparkSession): Unit ={
    // createGlobalTempView 来创建 Application 级别的临时表
    ratingsDataFrame.createGlobalTempView ("ratings")
    usersDataFrame.createGlobalTempView("users")

    val sql =
      """
        |SELECT Gender,
        |       Age,
        |       count(*)
        |FROM global_temp.users u
        |JOIN global_temp.ratings AS r on u.UserID = r.UserID
        |WHERE MovieID = 1193
        |GROUP BY Gender,
        |         Age
      """.stripMargin
    spark.sql(sql).show(10)

    import spark.sqlContext.implicits._
    ratingsDataFrame.select("MovieID", "Rating")
    .groupBy("MovieID").avg("Rating")
    // 接着我们可以使用 "$" 符号把引号里的字符串转换成列来实现相对复杂的功能，例如，下面
    // 我们把 avg(Rating) 作为排序的字段降序排列
//    .orderBy( $"avg(Rating)".desc).show(10)

     // DataFrame 和 RDD 的混合编程
     // 这里直接使用 DataFrame 的 rdd 方法转到 RDD 里操作
     .rdd.map(row => (row ( 1) , (row ( 0) , row ( 1 ) ) ) )
     .sortBy(_._1.toString.toDouble,false)
     .map(tuple => tuple._2)
     .collect.take(10).foreach(println)
  }

  /**
    * 通过 DataSet 实战电影点评系统案例
    * 找出观看某部电影的不同性别不同年龄的人数
    */
  def genderAndAgeCountMovieByDataSet(spark: SparkSession, users: RDD[String], ratings: RDD[String], movies: RDD[String]): Unit = {

    // 然后把数据封装进这些 Class:
    val usersForDSRDD: RDD[User] = users.map(_.split("::"))
      .map(line => User(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    // 最后直接创建 Dataset
    import spark.implicits._
    val usersDataSet: Dataset[User] = spark.createDataset(usersForDSRDD)
//    usersDataSet.show(10)

    val ratingsForDsRDD: RDD[Rating] = ratings.map(_.split("::"))
      .map(line => Rating(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))

    val ratingsDataSet: Dataset[Rating] = spark.createDataset(ratingsForDsRDD)

    // 下面的实现代码和使用 DataFrame 方法几乎完全一样(把 DataFrame 换成 Dataset 即可)
    ratingsDataSet.filter(s" MovieID = 1193")
      .join(usersDataSet, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age").count()
      .orderBy($"Gender".desc, $"Age").show(10)
  }

  case class User(UserID: String, Gender: String, Age: String, OccupationID: String, Zip_Code: String)
  case class Rating(UserID: String, MovieID: String, Rating: Double, Timestamp: String)
}
