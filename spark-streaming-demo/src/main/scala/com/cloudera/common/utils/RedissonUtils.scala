package com.cloudera.common.utils

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.concurrent.TimeUnit

import io.netty.util.CharsetUtil
import org.redisson.Redisson
import org.redisson.api.BatchOptions.ExecutionMode
import org.redisson.api._
import org.redisson.client.codec.{Codec, StringCodec}
import org.redisson.config.Config

object RedissonUtils extends Serializable {

  private final val properties = PropertiesScalaUtils.loadProperties("passenger_flow.properties")


  def getRedissonClient(): RedissonClient = {
    getRedissonClient(getConfig(new StringCodec(CharsetUtil.UTF_8)))
  }

  def getRedissonClient(config: Config): RedissonClient = {
    Redisson.create(config)
  }

  def getConfig(codec: Codec): Config ={
    val redisHost = properties.getProperty("redis.redisson.host").split(",")
    val redisPassword = properties.getProperty("redis.password")

    val config = new Config()
    config.setCodec(codec)
      .useClusterServers()
      .setScanInterval(2000)
      .addNodeAddress(redisHost: _*)
      .setPassword(redisPassword)
    config
  }



  def createBatch(redisson: RedissonClient): RBatch = {
    val options = BatchOptions.defaults()
    // 原子化（事务）批量执行所有的命令
    options.executionMode(ExecutionMode.IN_MEMORY)
    // 告知Redis不用返回结果（可以减少网络用量）
    options.skipResult // 将写入操作同步到从节点
    // 同步到1个从节点，等待时间为1秒钟
    options.syncSlaves(1, 1, TimeUnit.SECONDS)
    // 处理结果超时为2秒钟
    //    options.responseTimeout(2, TimeUnit.SECONDS)
    // 命令重试等待间隔时间为2秒钟
    options.retryInterval(2, TimeUnit.SECONDS)
    // 命令重试次数，仅适用于未发送成功的命令
    options.retryAttempts(3)
    val batch: RBatch = redisson.createBatch(options)
    batch
  }


  def createTransaction(redisson: RedissonClient): RTransaction = {
    val options = TransactionOptions.defaults()
    options.responseTimeout(3, TimeUnit.SECONDS)
    options.retryAttempts(3)
    options.retryInterval(2, TimeUnit.SECONDS)
    options.timeout(5, TimeUnit.SECONDS)
    val transaction: RTransaction = redisson.createTransaction(options)
    transaction
  }

  def checkReidsClusterKey(key: String): Unit = {
    var cal: Calendar = Calendar.getInstance()
    val hour = new SimpleDateFormat("HH").format(cal.getTime())
    val min = new SimpleDateFormat("mm").format(cal.getTime())

    val list = new scala.collection.mutable.ListBuffer[String]

    for (i <- 0 until hour.toInt) {
      for (j <- 0 until 60) {
        var str = "%02d".format(i) + ":" + "%02d".format(j)
        list ++= List(str)
      }
    }
    for (j <- 0 until min.toInt + 1) {
      var str = hour + ":" + "%02d".format(j)
      list ++= List(str)
    }
    println(list.toList.size)

    val redisson: RedissonClient = getRedissonClient()

    val rMap: RMap[String, String] = redisson.getMap(key)
    val map: util.Map[String, String] = rMap.readAllMap()
    println(map.size)
    for (s <- list) {
      if (!map.containsKey(s)) {
        println(s)
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val redisson: RedissonClient = getRedissonClient()
//    checkReidsClusterKey("passenger_flow:2019:11:13:in:87b0869011504dffad17911627f3d933")
//    redisson.getKeys().deleteByPattern("passenger_flow:2019:11:1*")
    val long: Long = System.currentTimeMillis()



//    val xaResource: Nothing = redisson.getXAResource
//    globalTransaction.enlistResource(xaResource)
//
//    val transaction: RTransaction = xaResource.getTransaction

//    redisson.getKeys().deleteByPattern("test:test-*")
//    val transaction: RTransaction = createTransaction(redisson)
//    val batch: RBatch = createBatch(redisson)
//    try{
//
//      for (i <- 0 until 100) {
//        val buket: RBucketAsync[String] = batch.getBucket("test:test-" + i)
//        buket.setAsync(i + "", 3600, TimeUnit.SECONDS)
//      }
////      1/0
//      batch.executeAsync()
//    }catch {
//      case e: Exception => {
//        println("计算异常", e)
//      }
//    }


    // 批量删除
    //    redisson.getKeys().deleteByPattern("passenger_flow:2019*")
//    val keysByPattern: lang.Iterable[String] = redisson.getKeys.getKeysByPattern("passenger_flow:2019:10:21:in:87b0869011504dffad17911627f3d93*")
//    val iterator: Iterator[String] = keysByPattern.iterator
//    while (iterator.hasNext) {
//      println(iterator.next)
//    }

//    val iterable: lang.Iterable[String] = redisson.getKeys().getKeysByPattern("test:*")
//
//    var i = 0
//    while (iterable.iterator().hasNext) {
//      //      val key: String = iterable.iterator.next
//      i = i + 1
//    }
//    println(i)
    val batch: RBatch = createBatch(redisson)

    for (i <- 0 until 10) {
      val batchBuket: RBucketAsync[String] = batch.getBucket("test:test-" + i)
      batchBuket.setAsync(i + "", 3600, TimeUnit.SECONDS) // 单条删除
      //      batchBuket.deleteAsync()
    }
//    batch.executeAsync()
    println(System.currentTimeMillis() - long)
    println("=======================================")
//    System.exit(0)
  }

  //      def main(args: Array[String]): Unit = {
  //        val redisson: RedissonClient = getRedissonClient()
  //        val long: Long = System.currentTimeMillis()
  //
  //        //    val batch: RBatch = redisson.createBatch
  //        //    for (i <- 0 until 100) {
  //        //      val batchBuket: RBucketAsync[String] = batch.getBucket("test:" + i)
  //        //      batchBuket.setAsync(i + "", ConfigUtils.getProperty("redis.device.expire").toLong, TimeUnit.SECONDS)
  //        ////      batchBuket.expireAtAsync(ConfigUtils.getProperty("redis.device.expire").toLong)
  //        ////      batchBuket.deleteAsync()
  //        //    }
  //        //        val batchMap: RMapAsync[String, String] = batch.getMap("hello")
  //        //    batchMap.putAsync("0","1234")
  //        //    batchMap.putAsync("1","1234")
  //        //    batchMap.putAsync("2","1234")
  //        //    batchMap.putAsync("3","1234")
  //        //    batchMap.expireAsync(3600,TimeUnit.SECONDS)
  //
  //        //    for (i <- 0 until 20) {
  //        //      val str: String = String.format("%02d", Integer.valueOf(i))
  //        //      val strings: util.Collection[String] = redisson.getKeys().findKeysByPattern("passenger_flow:307100044b8fa5da00"+str+":2019*")
  //        //      val iterator: util.Iterator[String] = strings.iterator()
  //        //      while(iterator.hasNext){
  //        //        val key: String = iterator.next()
  //        //        val batchBuket: RBucketAsync[String] = redisson.getBucket(key)
  //        //        batchBuket.deleteAsync()
  //        //      }
  //        //    }
  //        //        batch.execute()
  //
  //        val map1: util.Map[String, String] = redisson.getMap("passenger_flow:2019:in:87b0869011504dffad17911627f3d933").readAllMap()
  //        val map2: util.Map[String, String] = redisson.getMap("passenger_flow:2019:out:87b0869011504dffad17911627f3d933").readAllMap()
  //        val map3: util.Map[String, String] = redisson.getMap("passenger_flow:2019:10:in:87b0869011504dffad17911627f3d933").readAllMap()
  //        val map4: util.Map[String, String] = redisson.getMap("passenger_flow:2019:10:out:87b0869011504dffad17911627f3d933").readAllMap()
  //        println(map1)
  //        println(map2)
  //        println(map3)
  //        var sum1 = 0
  //
  //
  //        import scala.collection.JavaConversions._
  //        for(num <- map3.values()){
  //          sum1 += num.toInt
  //        }
  //        println("sum1===="+ sum1)
  //        var sum2 = 0
  //        for(num <- map4.values()){
  //          sum2 += num.toInt
  //        }
  //        println("sum2===="+ sum2)
  //        println(map4)
  //
  //  //      val str: String = BigScreenStat.getBigScreenStatResult(redisson)
  //        //    println(str)
  //        println("=======================================================")
  //
  //        val map5: util.Map[String, String] = redisson.getMap("passenger_flow:2019:10:18:in:87b0869011504dffad17911627f3d933").readAllMap()
  //        val map6: util.Map[String, String] = redisson.getMap("passenger_flow:2019:10:18:out:87b0869011504dffad17911627f3d933").readAllMap()
  //        val map7: util.Map[String, String] = redisson.getMap("passenger_flow:2019:10:21:in:87b0869011504dffad17911627f3d933").readAllMap()
  //        val map8: util.Map[String, String] = redisson.getMap("passenger_flow:2019:10:21:out:87b0869011504dffad17911627f3d933").readAllMap()
  //
  //
  //        var sum16in = 0
  //        for(num <- map5.values()){
  //          sum16in += num.toInt
  //        }
  //        println("sum16in===="+ sum16in)
  //        var sum16out = 0
  //        for(num <- map6.values()){
  //          sum16out += num.toInt
  //        }
  //        println("sum16out===="+ sum16out)
  //
  //        var sum17in = 0
  //        for(num <- map7.values()){
  //          sum17in += num.toInt
  //        }
  //        println("sum17in===="+ sum17in)
  //        var sum17out = 0
  //        for(num <- map8.values()){
  //          sum17out += num.toInt
  //        }
  //        val l: Long = redisson.getKeys().deleteByPattern("passenger_flow:2019*")
  //        //    val l1: Long = redisson.getKeys().deleteByPattern("passenger_flow:201*")
  //        //    val l: Long = redisson.getKeys().deleteByPattern("passenger_flow:2019:*")
  //        println("sum17out===="+ sum17out)
  //        println(System.currentTimeMillis() - long)
  //      }
}
