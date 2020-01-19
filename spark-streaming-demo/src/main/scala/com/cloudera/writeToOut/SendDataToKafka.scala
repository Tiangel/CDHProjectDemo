package com.cloudera.writeToOut

import com.cloudera.common.KafkaSink
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.Map

object SendDataToKafka extends App {

  private val logger: Logger = Logger.getLogger(this.getClass)

  val sparkConf = new SparkConf().setAppName("KafkaOperation")
    .setMaster("spark://localhost:7077")
    .set("spark.local.dir", "./tmp")
    .set("spark.streaming.kafka.maxRatePerPartition", "10")

  val ssc = new StreamingContext(sparkConf, Seconds(2))

  // Create direct kafka stream with brokers and topics
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9091,localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "kafkaOperationGroup",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  // 初始化KafkaSink,并广播
  val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
    val kafkaProducerConfig: Map[String, Object] = kafkaParams
    if (logger.isInfoEnabled) {
      logger.info("kafka producer init done!")
    }
    ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
  }

  val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](
    ssc, PreferConsistent, Subscribe[String, String](List("kafkaOperation"), kafkaParams)
  )

  val nameAddrStream = kafkaDirectStream.map(_.value).filter(record => {
    val tokens = record.split("\t") // addr type 0
    tokens(2).toInt == 0
  }).map(record => {
    val tokens = record.split("\t")
    (tokens(0), tokens(1))
  })

  val namePhoneStream = kafkaDirectStream.map(_.value).filter(record => {
    val tokens = record.split("\t") // phone type 1
    tokens(2).toInt == 1
  }).map(record => {
    val tokens = record.split("\t")
    (tokens(0), tokens(1))
  })

  val nameAddrPhoneStream = nameAddrStream.join(namePhoneStream).map(record => {
    s"姓名：${record._1}, 地址：${record._2._1}, 电话：${record._2._2}"
  })
  nameAddrPhoneStream.print()

  nameAddrPhoneStream.foreachRDD(record => {
    // 将统计数据发送到Kafka
    record.foreach(result => {
      kafkaProducer.value.send(
        "topic_mame",
        result
      );
    })
  })

  // 开始运算
  ssc.start()
  ssc.awaitTermination()
}
