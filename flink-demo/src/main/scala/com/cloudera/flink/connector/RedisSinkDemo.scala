package com.cloudera.flink.connector

import java.net.{InetSocketAddress, MalformedURLException}
import java.util
import java.util.Optional

import cn.hutool.core.date.DateUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}


object RedisSinkDemo {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val socketHost: String = "bigdata-dev-kafka-01"
    val socketPort: Int = 7777
    val redisClusterHosts: String = "bigdata-dev-redis-01:7000,bigdata-dev-redis-02:7000,bigdata-dev-redis-03:7000"

    val socketDs: DataStream[String] = streamEnv.socketTextStream(socketHost, socketPort)

    val wordCount: DataStream[(String, Int)] =
    //      socketDs.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
      socketDs.flatMap(_.trim.split(" ")).filter(x => !"".equals(x)).map((_, 1)).keyBy(0)

    //连接redis的配置

    val nodes = new util.HashSet[InetSocketAddress](getRedisClusterAddresses(redisClusterHosts))
    //    val nodes = new util.HashSet[InetSocketAddress](
    //      util.Arrays.asList(
    //        InetSocketAddress.createUnresolved(redisHost, redisPort)
    //      ))
    val config: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
      .setNodes(nodes).build()
    //写入redis
    wordCount.addSink(new RedisSink[(String, Int)](config, new MyRedisMapper))
    streamEnv.execute("Redis Sink Demo")
  }

  @throws[MalformedURLException]
  def getRedisClusterAddresses(hosts: String): util.List[InetSocketAddress] = {
    val hostList: Array[String] = hosts.split(",", -1)
    val addresses: util.List[InetSocketAddress] = new util.ArrayList[InetSocketAddress]()
    for (host <- hostList) {
      val parts: Array[String] = host.split(":", 2)
      if (parts.length > 1) {
        addresses.add(InetSocketAddress.createUnresolved(parts(0), parts(1).toInt))
      } else {
        throw new MalformedURLException("invalid redis hosts format")
      }
    }
    addresses
  }
}

class MyRedisMapper extends RedisMapper[(String, Int)] {
  val ZSET_NAME_PREFIX: String = "RANKING:"

  override def getCommandDescription: RedisCommandDescription = {
    //    new RedisCommandDescription(RedisCommand.HSET, "wordcount")
    new RedisCommandDescription(RedisCommand.ZINCRBY, ZSET_NAME_PREFIX)
  }

  override def getKeyFromData(t: (String, Int)): String = {
    t._1
  }

  override def getValueFromData(t: (String, Int)): String  = {
    t._2.toString
  }

  override def getAdditionalKey(data: (String, Int)): Optional[String] = {
    val key = ZSET_NAME_PREFIX + DateUtil.today() + ":" + "TEST"
    Optional.of(key)
  }
}

