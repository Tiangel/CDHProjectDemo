package com.cloudera.common.utils

import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}
import org.redisson.api.{RMap, RedissonClient}

import scala.collection.JavaConversions._
import scala.collection.immutable.Map

object KafkaRedisUtils {
  private val logger: Logger = Logger.getLogger(this.getClass)

  /**
    * 创建 DirectStream
    * @param streamingContext
    * @param kafkaParams   kafka参数
    * @param module    模块名
    * @param groupId   消费者组
    * @param topics    topics
    * @return
    */
  def createDirectStream(streamingContext: StreamingContext, kafkaParams: Map[String, Object],
                         module: String, groupId: String,topics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {

    //读取 topic 的 offset
    val storedOffsets = readOffsetsByJedis(module, groupId, kafkaParams, topics)

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = storedOffsets match {
      //上次未保存offsets
      case None => {
        KafkaUtils.createDirectStream[String, String](
          streamingContext,
          PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
        )
      }
      case Some(fromOffsets) => {
        KafkaUtils.createDirectStream[String, String](
          streamingContext,
          PreferConsistent,
          // 指定分区消费，无法动态感知分区变化
//          ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
          ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets)
        )
      }
    }
    kafkaStream
  }


  /**
    * 使用 Jedis 读取 Redis 中的 offset
    * @param module
    * @param groupId
    * @param kafkaParams
    * @param topics
    * @return
    */
  def readOffsetsByJedis(module: String, groupId: String,kafkaParams: Map[String, Object], topics: Array[String]): Option[Map[TopicPartition, Long]] = {
    logger.info("Reading offsets from Redis")
    val jedis = JedisPoolUtils.getPool.getResource

    //设置每个分区起始的Offset
    var fromOffSets: Map[TopicPartition, Long] = Map()
    try {
      topics.foreach(topic => {
        var topicFromOffsets: Map[TopicPartition, Long] = Map()
        val key = s"${module}:${groupId}:${topic}"
        if (jedis.exists(key)) {
          val offsetMap: util.Map[String, String] = jedis.hgetAll(key)
          offsetMap.map(record => {
            //判断field(即分区)是否存在
            if(jedis.hexists(key,record._1)){
              logger.info(s"topic ${topic} partition ${record._1} get lastSavedOffset from redis: ${record._2}")
              topicFromOffsets += new TopicPartition(topic, record._1.toInt) -> record._2.toLong
            }else{
              jedis.hset(key,record._1,"0")
            }
          })
        }
        fromOffSets ++= topicFromOffsets
      })
    } catch {
      case e: Exception => logger.error("readOffsets error ", e)
        System.exit(1)
    }finally {
      jedis.close()
    }
    if (fromOffSets.isEmpty) {
      None
    } else {
      Some(fromOffSets)
    }
  }

  /**
    * 使用 Redisson 读取 Redis 中的
    * @param module
    * @param groupId
    * @param kafkaParams
    * @param topics
    * @return
    */
  def readOffsetsByRedisson(module: String, groupId: String,kafkaParams: Map[String, Object], topics: Array[String]): Option[Map[TopicPartition, Long]] = {
    logger.info("Reading offsets from Redis")
    val redisson: RedissonClient = RedissonUtils.getRedissonClient()

    //设置每个分区起始的Offset
    var fromOffSets: Map[TopicPartition, Long] = Map()
    try {
      topics.foreach(topic => {
        var topicFromOffsets: Map[TopicPartition, Long] = Map()
        val key = s"${module}:${groupId}:${topic}"
        val rMap: RMap[String, String] = redisson.getMap(key)
        if (rMap.isExists) {
          val offsetMap: util.Set[util.Map.Entry[String, String]] = rMap.readAllEntrySet()
          val iterator: util.Iterator[util.Map.Entry[String, String]] = offsetMap.iterator
          while(iterator.hasNext) {
            val record: util.Map.Entry[String, String] = iterator.next()
            if(rMap.containsKey(record.getKey)){
              topicFromOffsets += new TopicPartition(topic, record.getKey.toInt) -> record.getValue.toLong
            }else{
              rMap.put(record.getKey,"0")
            }
          }
        }
        fromOffSets ++= topicFromOffsets
      })
    } catch {
      case e: Exception => logger.error("readOffsets error ", e)
        System.exit(1)
    }
    if (fromOffSets.isEmpty) {
      None
    } else {
      Some(fromOffSets)
    }
  }

}
