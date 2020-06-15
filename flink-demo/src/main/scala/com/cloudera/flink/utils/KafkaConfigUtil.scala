package com.hdb.flink.utils

;


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import java.util.HashMap
import java.util.List
import java.util.Map
import java.util.Properties

import com.hdb.flink.common.constant.Constant
import org.apache.kafka.common.serialization.StringDeserializer;


object KafkaConfigUtil {


  def buildKafkaProps(): Properties = {
    buildKafkaProps(ParameterTool.fromSystemProperties())
  }
  def buildKafkaProps(parameterTool: ParameterTool): Properties = {
    val comsumerProps = parameterTool.getProperties;
    comsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.get(Constant.BOOTSTRAP_SERVERS, Constant.DEFAULT_BOOTSTRAP_SERVERS))
    comsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    comsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    comsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, parameterTool.get(Constant.KAFKA_GROUP_ID, Constant.DEFAULT_KAFKA_GROUP_ID))
    comsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, parameterTool.get(Constant.KAFKA_AUTO_OFFSET, Constant.DEFAULT_KAFKA_AUTO_OFFSET))
    comsumerProps
    comsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, parameterTool.get(Constant.KAFKA_AUTO_COMMIT, Constant.DEFAULT_KAFKA_AUTO_COMMIT))
  }

}
