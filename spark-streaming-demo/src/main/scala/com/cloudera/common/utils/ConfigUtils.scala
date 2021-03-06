package com.cloudera.common.utils

import java.util

import com.google.gson.Gson
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.redisson.api.RBucket


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

  /**
    * 获取保存offset的 Zookeeper 相关参数
    * @return
    */
  def getZkClient(): ZkClient = {
    val zkUrl = properties.getProperty("zookeeper.url")
    val sessionTimeout = properties.getProperty("zookeeper.session.timeout")
    val connectionTimeout = properties.getProperty("zookeeper.connection.timeout")
    ZkUtils.createZkClient(zkUrl, sessionTimeout.toInt, connectionTimeout.toInt)
  }

  /**
    * 客流组织关系配置获取及转换 (设备id, 组织id, 正反向关系)
    * @param jedis
    * @return
    */
  def getPassengerFlowProFile(spark: SparkSession): Array[(String, String, String)] = {

    // 从Redis中获取组织关系配置
    val flowProfileKey: String = properties.getProperty("redis.passengerflowprofile.key")
    var flowProfile: String = ""
    val redisson = RedissonUtils.getRedissonClient
    try {
      val rBucket: RBucket[String] = redisson.getBucket(flowProfileKey)
      if (rBucket.isExists){
        flowProfile = rBucket.get()
      }
      /*(jedisCluster.exists(flowProfileKey)) {
        flowProfile = jedisCluster.get(flowProfileKey)
      } */else {
        // TODO:从hive中取出最新的组织关系配置
        val df: DataFrame = spark.sql("SELECT deal_data,sink_date FROM passenger_flow.passenger_flow_profile ORDER BY sink_date DESC limit 1")
        val rows: util.List[Row] = df.collectAsList()
        if (!rows.isEmpty) {
          val row: Row = rows.get(0)
          flowProfile = row.get(0).toString
        }
      }
      val gson = new Gson()
      val profiles: Array[PassengerFlowProfile] = gson.fromJson(flowProfile, classOf[Array[PassengerFlowProfile]])
      val list = scala.collection.mutable.ArrayBuffer[(String, String, String)]()
      if (profiles != null && profiles.length > 0) {
        profiles.foreach(s => {
          if (s.devices != null && s.devices.length > 0) {
            s.devices.foreach(r => {
              list ++= Array((r.deviceId.toLowerCase, s.organizId, r.countType))
            })
          }
        })
      }
      list.toArray
    } catch {
      case e: Exception => {
        logger.error("获取解析客流配置信息异常", e)
        throw e
      }
    } finally {
      redisson.shutdown()
    }
  }

  /**
    * 组装设备对应的客流组织关系配置 (设备id, 正向组织id, 反向组织id)
    * @param jedis
    * @return
    */
  def getPassengerFlowDeviceProFile(tuples: Array[(String, String, String)]): Map[String, (String, String)] = {
    var map: Map[String, (String, String)] = Map[String, (String, String)]()
    tuples.foreach(r => {
      var tuple: (String, String) = map.getOrElse(r._1,("",""))
      if(r._3 == "1"){
        if(tuple._1 == "") {
          tuple = (r._2,tuple._2)
        }else {
          tuple = (tuple._1 + "," + r._2,tuple._2)
        }
      }else{
        if(tuple._2 == "") {
          tuple = (tuple._1,r._2)
        }else {
          tuple = (tuple._1 ,tuple._2 + "," + r._2)
        }
      }
      map += (r._1 -> tuple)
    })
    map
  }
}
