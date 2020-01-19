package com.cloudera.common

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  // 这样能够避免运行时产生 NotSerializableExceptions 异常
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        // 确保在 Executor 的 JVM 关闭前 Kafka producer 将缓存中的所有信息写入 Kafka
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}