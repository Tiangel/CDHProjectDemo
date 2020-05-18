package com.cloudera.flink

import java.net.InetSocketAddress
import java.util

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisDataType, RedisMapper}


object RedisSinkDemo {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val checkpointPath: String = "file:\\\\E:\\CDHProjectDemo\\flink-demo\\data\\test\\checkpoint"

    val backend = new RocksDBStateBackend(checkpointPath)
    streamEnv.setStateBackend(backend)
    streamEnv.enableCheckpointing(1000)
    // 配置重启策略
//    streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(60, Time.of(10,
//      TimeUnit.SECONDS)))
//    streamEnv.


    val host: String = "bigdata-dev-kafka-01"
    val port: Int = 7777

    val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)

    val wordCount: DataStream[(String, Int)] =
    socketDs.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    //连接redis的配置
    val nodes =  new util.HashSet[InetSocketAddress](
      util.Arrays.asList(
        new InetSocketAddress("10.101.40.203",7000)
      ))
    val config: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
      .setNodes(nodes).build()
    //写入redis
    wordCount.addSink(new RedisSink[(String,Int)](config,new MyRedisMapper))
    streamEnv.execute()
  }
}

class MyRedisMapper extends RedisMapper[(String,Int)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"wordcount")
  }

  override def getKeyFromData(t: (String, Int)): String = {
    t._1
  }

  override def getValueFromData(t: (String, Int)): String = {
    t._2.toString
  }
}