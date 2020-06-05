package com.cloudera.flink.java.demo

import java.net.InetSocketAddress
import java.util

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


object RedisSinkDemo {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val socketHost: String = "bigdata-dev-kafka-01"
    val socketPort: Int = 7777
    val redisHost: String = "bigdata-dev-kafka-01"
    val redisPort: Int = 7777

    val socketDs: DataStream[String] = streamEnv.socketTextStream(socketHost, socketPort)

    val wordCount: DataStream[(String, Int)] =
//      socketDs.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
      socketDs.flatMap(_.split(" ")).map((_, 1)).keyBy(0)

    //连接redis的配置
    val nodes = new util.HashSet[InetSocketAddress](
      util.Arrays.asList(
        new InetSocketAddress(redisHost, redisPort)
      ))
    val config: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
      .setNodes(nodes).build()
    //写入redis
    wordCount.addSink(new RedisSink[(String, Int)](config, new MyRedisMapper))
    streamEnv.execute("Redis Sink Demo")
  }
}

class MyRedisMapper extends RedisMapper[(String, Int)] {
  override def getCommandDescription: RedisCommandDescription = {
//    new RedisCommandDescription(RedisCommand.HSET, "wordcount")
    new RedisCommandDescription(RedisCommand.HINCRBY, "wordcount")
  }

  override def getKeyFromData(t: (String, Int)): String = {
    t._1
  }

  override def getValueFromData(t: (String, Int)): String = {
    t._2.toString
  }
}
