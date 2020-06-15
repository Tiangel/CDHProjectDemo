package com.cloudera.flink.java.utils

import java.util.Properties

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger


object ConfigUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private final val properties = PropertiesScalaUtils.loadProperties("passenger_flow.properties")

  /**
   * 获取 passenger_flow.properties配置文件中 key对应的值
   *
   * @param key
   * @return
   */
  def getProperty(key: String): String = {
    properties.getProperty(key)
  }

  /**
   * 获取 sparkstreaming 消费kafka客流上报数据的消费者参数
   *
   * @return
   */
  def getKafkaConsumerParams(groupId: String): Properties = {
    val props = new Properties

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.bootstrap.servers"))
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("kafka.auto.offset.reset"))
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

//    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.getProperty("kafka.security.protocol"))
//    props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.getProperty("kafka.client.truststore.jks"))
//    props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getProperty("kafka.client.truststore.jks.password"))
//    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, properties.getProperty("kafka.client.keystore.jks"))
//    props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.getProperty("kafka.client.keystore.jks.password"))
    props
  }

  /**
   * 获取 sparkstreaming 统计结果写入大屏生成者参数
   *
   * @return
   */
  def getKafkaProducerParams(): Map[String, Object] = {
    Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.bootstrap.servers"),
      ProducerConfig.ACKS_CONFIG -> properties.getProperty("kafka.acks"),
      ProducerConfig.RETRIES_CONFIG -> properties.getProperty("kafka.retries"),
      ProducerConfig.BATCH_SIZE_CONFIG -> properties.getProperty("kafka.batch.size"),
      ProducerConfig.LINGER_MS_CONFIG -> properties.getProperty("kafka.linger.ms"),
      ProducerConfig.BUFFER_MEMORY_CONFIG -> properties.getProperty("kafka.buffer.memory"),
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
      //CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> properties.getProperty("kafka.security.protocol"),
      //SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> properties.getProperty("kafka.client.truststore.jks"),
      //SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> properties.getProperty("kafka.client.truststore.jks.password"),
      //SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> properties.getProperty("kafka.client.keystore.jks"),
      //SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> properties.getProperty("kafka.client.keystore.jks.password")
    )
  }

}

