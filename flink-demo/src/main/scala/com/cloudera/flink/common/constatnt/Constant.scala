package com.hdb.flink.common.constant

object Constant {


  val CHARSET_NAME = "UTF-8"

  // kafka
  val KAFKA_TOPICS = "topics"
  val KAFKA_RETRIES: String = "retries"


  val DEFAULT_BOOTSTRAP_SERVERS = "bigdata-dev-kafka-01:9092,bigdata-dev-kafka-02:9092,bigdata-dev-kafka-03:9092"
  val BOOTSTRAP_SERVERS = "bootstrap.servers"
  val DEFAULT_KAFKA_GROUP_ID = "Flink_Test"
  val KAFKA_GROUP_ID = "group.id"
  // earliest latest none
  val DEFAULT_KAFKA_AUTO_OFFSET = "earliest"
  val KAFKA_AUTO_OFFSET = "reset"
  val DEFAULT_KAFKA_AUTO_COMMIT = "false"
  val KAFKA_AUTO_COMMIT = "enable.auto.commit"


  // 是否开启checkpoint
  val STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable"
  // checkpoint 目录
  val STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir"
  // checkpoint 类型
  val STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type"









}
