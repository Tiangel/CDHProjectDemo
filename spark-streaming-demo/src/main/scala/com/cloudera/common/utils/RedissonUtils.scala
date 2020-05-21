package com.cloudera.common.utils

import java.util.concurrent.TimeUnit

import io.netty.util.CharsetUtil
import org.redisson.Redisson
import org.redisson.api.BatchOptions.ExecutionMode
import org.redisson.api._
import org.redisson.client.codec.{Codec, StringCodec}
import org.redisson.config.{Config, ReadMode}

object RedissonUtils extends Serializable {

  private final val properties = PropertiesScalaUtils.loadProperties("passenger_flow.properties")


  def getRedissonClient(): RedissonClient = {
    getRedissonClient(new StringCodec(CharsetUtil.UTF_8))
  }

  def getRedissonClient(codec: Codec): RedissonClient = {

    val redisHost = properties.getProperty("redis.redisson.host").split(",")
    val redisPassword = properties.getProperty("redis.password")

    val config = new Config()
    config.setCodec(codec).useClusterServers()
      // 集群状态扫描间隔时间，单位是毫秒
      .setScanInterval(3000)
      .addNodeAddress(redisHost: _*)
      .setPassword(redisPassword)
      .setMasterConnectionPoolSize(64)
      .setSlaveConnectionPoolSize(64)
      .setReadMode(ReadMode.SLAVE)
    Redisson.create(config)
  }

  def createBatch(redisson: RedissonClient): RBatch = {
    val options = BatchOptions.defaults()
    // 原子化（事务）批量执行所有的命令
    options.executionMode(ExecutionMode.IN_MEMORY)
    // 告知Redis不用返回结果（可以减少网络用量）
    options.skipResult
    // 将写入操作同步到从节点
    // 同步到1个从节点，等待时间为1秒钟
    options.syncSlaves(1, 1, TimeUnit.SECONDS)
    // 处理结果超时为2秒钟
    options.responseTimeout(2, TimeUnit.SECONDS)
    // 命令重试等待间隔时间为2秒钟
    options.retryInterval(2, TimeUnit.SECONDS)
    // 命令重试次数，仅适用于未发送成功的命令
    options.retryAttempts(3)
    redisson.createBatch(options)
  }
}
