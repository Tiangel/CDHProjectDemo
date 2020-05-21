package com.cloudera.flink.utils

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger



object ConfigUtils {

  private val logger: Logger = Logger.getLogger(this.getClass)

  private final val properties = PropertiesScalaUtils.loadProperties("passenger_flow.properties")

  /**
   * 获取 passenger_flow.properties配置文件中 key对应的值
   * @param key
   * @return
   */
  def getProperty(key: String): String = {
    properties.getProperty(key)
  }

  /**
   * 获取 sparkstreaming 消费kafka客流上报数据的消费者参数
   * @return
   */
  def getKafkaConsumerParams(groupId: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("kafka.bootstrap.servers"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> properties.getProperty("kafka.auto.offset.reset"),
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
      //CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> properties.getProperty("kafka.security.protocol"),
      //SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> properties.getProperty("kafka.client.truststore.jks"),
      //SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> properties.getProperty("kafka.client.truststore.jks.password"),
      //SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG -> properties.getProperty("kafka.client.keystore.jks"),
      //SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG -> properties.getProperty("kafka.client.keystore.jks.password")
    )
  }

  /**
   * 获取 sparkstreaming 统计结果写入大屏生成者参数
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

