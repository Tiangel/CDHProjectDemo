package com.cloudera.savekafkaoffset

import java.util.concurrent.TimeUnit

import com.cloudera.common.KafkaSink
import com.cloudera.common.utils.{ConfigUtils, KafkaRedisUtils, ParseFlowRecord, RedissonUtils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.redisson.api.{RBatch, RBucketAsync, RMapAsync, RedissonClient}

import scala.collection.immutable.Map

object PassengerFlowMatchApp {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val module: String = ConfigUtils.getProperty("redis.namespace.module.name")

    val groupId: String = ConfigUtils.getProperty("kafka.group.match.id")

    val topics: Array[String] = ConfigUtils.getProperty("kafka.passengerflow.topics").split(",")

    val kafkaParams: Map[String, Object] = ConfigUtils.getKafkaConsumerParams(groupId)

    System.setProperty("HADOOP_USER_NAME", "hive")
    val warehouseLocation = ConfigUtils.getProperty("hive.warehouse.dir")

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[2]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", args(0))
      .setAppName("PassengerFlow_Match_APP")

    val spark: SparkSession = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    @transient val sc = spark.sparkContext
    val streamingContext = new StreamingContext(sc, Seconds(args(1).toInt))

    val kafkaStream = KafkaRedisUtils.createDirectStream(streamingContext, kafkaParams, module, groupId, topics)

    // 初始化KafkaSink,并广播
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig: Map[String, Object] = ConfigUtils.getKafkaProducerParams()
      if (logger.isInfoEnabled) {
        logger.info("kafka producer init done!")
      }
      streamingContext.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    //开始处理批次消息
    kafkaStream.foreachRDD(rdd => {
      //获取当前批次的RDD的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      if (!rdd.isEmpty()) {
        // 获取redis连接
        val redisson: RedissonClient = RedissonUtils.getRedissonClient

        val profileTuples: Array[(String, String, String)] = ConfigUtils.getPassengerFlowProFile(spark)

        // 判断客流配置是否为空
        if (!profileTuples.isEmpty) {

          // 广播组织配置信息
          val broadcast: Broadcast[Map[String, (String, String)]] = streamingContext.sparkContext.broadcast(ConfigUtils.getPassengerFlowDeviceProFile(profileTuples))

          try {
            // 创建批处理
            val batch: RBatch = RedissonUtils.createBatch(redisson)
            // 获取kafka中的数据并解析成FlowRecord
            val result = ParseFlowRecord.processPassengerRecords(rdd)

            logger.info("==========> Total " + result.length + " events in this    batch ..")
            // 开始处理数据
            result.foreach(record => {
              //将设备数据存入redis
              val key = s"${module}:${record.deviceId}:${record.startTime}"

              val batchBuket: RBucketAsync[String] = batch.getBucket(key)
              batchBuket.setAsync(record.inNum + "|" + record.outNum.toString,ConfigUtils.getProperty("redis.device.expire").toLong, TimeUnit.SECONDS)

             // 读取每条数并添加配置然后发送到kafka
              val tuple: (String, String) = broadcast.value.getOrElse(record.deviceId,("",""))
              val passengerFlowMatch: String = s"${record.deviceId}|${record.cmdType}|${record.parentDeviceId}|${record.startTime}|${record.totalTime}|${record.inNum}|${record.outNum}|${tuple._1}|${tuple._2}";
              //将处理后的数据发送到Kafka
              kafkaProducer.value.send(
                ConfigUtils.getProperty("kafka.passengerflow.match.topic"),
//                ConfigUtils.getProperty("kafka.passengerflow.match.key"),
                passengerFlowMatch
              );
            })

            //更新offset到Redis中
            offsetRanges.foreach({ offsetRange =>
              logger.info("==========> partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
              val key = s"${module}:${groupId}:${offsetRange.topic}"
              //                jedisCluster.hset(key, offsetRange.partition.toString,offsetRange.untilOffset.toString)
//              val batchMap: RMapAsync[String, String] = batchBroadcast.value.getMap(key)
              val batchMap: RMapAsync[String, String] = batch.getMap(key)
              batchMap.putAsync(offsetRange.partition.toString, offsetRange.untilOffset.toString)
            })
            // 一次性执行所有命令
            batch.executeAsync()
          } catch {
            case e: Exception => {
              logger.error("客流统计计算异常", e)
            }
          } finally {
            // 关闭连接
            redisson.shutdown()
            //销毁广播变量
            broadcast.destroy()
          }
        }
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }
}
