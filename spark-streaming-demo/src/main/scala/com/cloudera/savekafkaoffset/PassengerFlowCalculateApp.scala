package com.cloudera.savekafkaoffset

import com.cloudera.common.KafkaSink
import com.cloudera.common.utils.{ConfigUtils, KafkaRedisUtils, ParseFlowRecord, RedissonUtils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.redisson.api._

import scala.collection.immutable.Map


object PassengerFlowCalculateApp {
  private val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val module: String = ConfigUtils.getProperty("redis.namespace.module.name")

    val groupId: String = ConfigUtils.getProperty("kafka.group.calculate.id")

    val topics: Array[String] = ConfigUtils.getProperty("kafka.passengerflow.match.topic").split(",")

    val kafkaParams = ConfigUtils.getKafkaConsumerParams(groupId)

    val conf = new SparkConf().setIfMissing("spark.master", "local[*]")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.kafka.maxRatePerPartition", args(0))
      .setAppName("PassengerFlow_Calculate_APP")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

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
        val redisson = RedissonUtils.getRedissonClient()

        logger.info("==========> Total " + rdd.count() + " events in this    batch ..")
        val result: RDD[(String, (Long, Long))] = ParseFlowRecord.processPassengerMatchRecords(rdd)

        //逐条处理消息
        try {
          // 创建批处理
          val batch: RBatch = RedissonUtils.createBatch(redisson)
          result.collect.foreach(record => {
            //  客流统计后台逻辑
//            LogicData.passengerflow(batch, record)
          })
          //更新offset到Redis中
          offsetRanges.foreach({ offsetRange =>
            logger.info("==========> partition : " + offsetRange.partition + " fromOffset:  "
              + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
            val key = s"${module}:${groupId}:${offsetRange.topic}"
            val batchMap: RMapAsync[String, String] = batch.getMap(key)
            batchMap.putAsync(offsetRange.partition.toString, offsetRange.untilOffset.toString)
          })
          // 一次性执行所有命令
          batch.executeAsync()

          // 读取Redis中大屏相关指标统计结果
          val bigScreenJson: String = "......................";
          logger.info("发送到大屏: ====> " + bigScreenJson)
          // 将统计数据发送到Kafka
          kafkaProducer.value.send(
            ConfigUtils.getProperty("Kafka.bigscreen.topic"),
            ConfigUtils.getProperty("kafka.bigscreen.passengerflow.key"),
            bigScreenJson
          );
        } catch {
          case e: Exception => {
            logger.error("客流统计计算异常", e)
          }
        } finally {
          //关闭连接
          redisson.shutdown()
        }
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }

}