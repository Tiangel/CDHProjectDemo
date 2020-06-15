package com.cloudera.flink.common.deserializationschema

import com.hdb.flink.common.constant.Constant
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicAndValueDeserializationSchema extends KafkaDeserializationSchema[TopicAndValue] with Serializable {
  override def isEndOfStream(t: TopicAndValue): Boolean = false

  override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): TopicAndValue = {
    new TopicAndValue(consumerRecord.topic(), new String(consumerRecord.value(),Constant.CHARSET_NAME))
  }

  override def getProducedType: TypeInformation[TopicAndValue] = {
    TypeInformation.of(new TypeHint[TopicAndValue]{})
  }

}

class TopicAndValue(var topic:String,var value:String) extends Serializable {

  override def toString: String = {
    s"[topic = ${this.topic}, value = ${this.value}]"
  }




}
object Test {

  def main (args: Array[String] ): Unit = {
    val value = new TopicAndValue("aaaa", "bbb")
    value.topic = "bbb"
    print(value)
  }
}


