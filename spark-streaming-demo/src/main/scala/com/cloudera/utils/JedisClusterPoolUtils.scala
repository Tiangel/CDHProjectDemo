package com.cloudera.utils

import java.util

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

case class RedisClusterConfig(redisHostAndPort: String, redisName: String, redisPassword: String, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int) extends  Serializable

/**
  * Redis 集群版连接池工具类
  */
object JedisClusterPoolUtils extends Serializable {

  @transient private var pool: JedisCluster = null

  def makePool(redisClusterConfig: RedisClusterConfig): Unit ={
    makePool(redisClusterConfig.redisHostAndPort, redisClusterConfig.redisName, redisClusterConfig.redisPassword, redisClusterConfig.redisTimeout, redisClusterConfig.maxTotal, redisClusterConfig.maxIdle, redisClusterConfig.minIdle)
  }

  def makePool(redisHostAndPort: String, redisName: String, redisPassword: String, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
    makePool(redisHostAndPort, redisName, redisPassword, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
  }

  def makePool(redisHostAndPort: String, redisName: String, redisPassword: String, redisTimeout: Int, maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
    if (pool == null) {
      val poolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)

      val nodes = new util.HashSet[HostAndPort]()
      val redisURLArray: Array[String] = redisHostAndPort.split(",")
      for (redisURL <- redisURLArray) {
        val hostAndPort = new HostAndPort(redisURL.split(":")(0),redisURL.split(":")(1).toInt)
        nodes.add(hostAndPort)
      }
      pool = new JedisCluster(nodes,redisTimeout.toInt,2000,5,redisPassword,poolConfig)

      val hook = new Thread {
        override def run = pool.close()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisCluster = {
    assert(pool != null)
    pool
  }
}
