package com.cloudera.flink.source

import java.util.Properties

import com.cloudera.flink.utils.ConfigUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceDemo {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    val groupId: String = "groupId"
    val topic: String = "t_topic"
    val charsetName = "UTF-8"
    val props: Properties = ConfigUtils.getKafkaConsumerParams(groupId);

    val stream = streamEnv.addSource(new FlinkKafkaConsumer[(String, String)](topic, new KafkaDeserializationSchema[(String, String)] {
      override def isEndOfStream(t: (String, String)): Boolean = false

      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        if (consumerRecord != null) {
          var key = "null"
          var value = "null"
          if (consumerRecord.key() != null) {
            key = new String(consumerRecord.key(), charsetName)
          }
          if (consumerRecord.value() != null) {
            value = new String(consumerRecord.value(), charsetName)
          }
          (key, value)
        } else {
          //如果kafka中的数据为空返回一个固定的二元组
          ("null", "null")
        }
      }

      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, props).setStartFromEarliest)

    stream.print()
    streamEnv.execute()
  }

}
