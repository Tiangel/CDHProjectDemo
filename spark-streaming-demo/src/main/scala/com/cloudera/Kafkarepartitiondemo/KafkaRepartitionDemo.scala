package com.cloudera.Kafkarepartitiondemo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 1. 直接消费新数据，离线修补堆压数据
 * 2. repartition (10 -> 100) 给足够多的资源，以便任务逐渐消费滞后的数据
 * 3. directDstream API 生成的是 kafkaRDD，读 rdd 与 kafka 分区一一对应
 *   改为一对多 repartition
 */
object KafkaRepartitionDemo {

  def main(args: Array[String]): Unit = {

    // 创建一个批处理时间是2s的context 要增加环境变量
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setIfMissing("spark.master", "local[2]")
    // 是否开启自动reparation
    sparkConf.set("enable.auto.repartition", "true")
    // 每个分区一次处理数据量
    sparkConf.set("per.partition.offsetrange.step", "100")
    // 每个分区临界值
    sparkConf.set("per.partition.offsetrange.threshold", "300")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 停用 broker 和 topic 创建的 DirectDstream
    val topicsSet = "eda_collector_flume_new".split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "bigdata-dev-kafka-01:9092,bigdata-dev-kafka-02:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "groupIdtestsss",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
    )
    val message = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )

    message.transform(rdd => {
      println("partition.size : " + rdd.getNumPartitions)
      rdd
    }).foreachRDD(rdd => {
      //      rdd.foreachPartition(each => print(1111))
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(o => {
        println(s"${o.topic} ===== ${o.partition} ===== ${o.fromOffset} ===== ${o.untilOffset}")
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
