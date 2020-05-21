package com.cloudera.common.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object ParseFlowRecord {

  private val logger: Logger = Logger.getLogger(this.getClass)

  def processPassengerRecords(rdd: RDD[ConsumerRecord[String, String]]): Array[PassengerFlowRecord] = {
    rdd.map(_.value()).flatMap(parsePassengerRecord).filter(_ != null).distinct().collect()
  }

  //解析客流每条数据，生成PassengerFlowRecord
  def parsePassengerRecord(line: String): Option[PassengerFlowRecord] = {
    val ary: Array[String] = line.split("\\|", -1);
    try {
      if (ary.length == 7) {
        val deviceId = ary(0).trim.toLowerCase()
        val cmdType = ary(1).trim
        val parentDeviceId = ary(2).trim
        val startTime = ary(3).trim.replace("-", "").replace(":", "").replace(" ", "")
        val totalTime = ary(4).trim.toInt
        val inNum = ary(5).trim.toInt
        val outNum = ary(6).trim.toInt
        return Some(PassengerFlowRecord(deviceId, cmdType, parentDeviceId, startTime, totalTime, inNum, outNum))
      }
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage)
      }
    }
    return None
  }

  //  def processPassengerMatchRecords(rdd: RDD[ConsumerRecord[String, String]]): Array[PassengerMatchRecord] = {
  //    rdd.map(_.value()).flatMap(processPassengerMatchRecord).filter(_ != null).distinct().collect()
  //  }
  def processPassengerMatchRecords(rdd: RDD[ConsumerRecord[String, String]]): RDD[(String, (Long, Long))] = {

    val rddCache = rdd.mapPartitions(x => for (y <- x if y.value() != null && y.value() != "") yield y.value())
      .distinct()
      .mapPartitions(x => for (y <- x) yield y.split("\\|", -1))
      .filter(x => (x(7) != null && x(8) != null))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val inRdd = rddCache.filter(x => x(7).trim != "")
      .mapPartitions(y => for (x <- y) yield (x(7), x(3).substring(0, 14), x(5), x(6)))

    val outRdd = rddCache.filter(x => x(8).trim != "")
      .mapPartitions(y => for (x <- y) yield (x(8), x(3).substring(0, 14), x(6), x(5)))

    // (组织id_时间,in,out)
    val result: RDD[(String, (Long, Long))] = inRdd.union(outRdd)
      .mapPartitions(x => for (y <- x) yield (y._1.split(",").toIterable, y._2, y._3, y._4))
      .flatMap(x => for (y <- x._1) yield (y + "_" + x._2, (x._3.toLong, x._4.toLong)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    rddCache.unpersist()
    result
  }

  /**
    * //解析客流匹配配置后的每条数据，生成PassengerMatchRecord
    *
    * @param line
    * @return
    */
  def processPassengerMatchRecord(line: String): Option[PassengerMatchRecord] = {
    val ary: Array[String] = line.split("\\|", -1);
    try {
      if (ary.length == 9) {
        val deviceId = ary(0).trim.toLowerCase()
        val cmdType = ary(1).trim
        val parentDeviceId = ary(2).trim
        val startTime = ary(3).trim.replace("-", "").replace(":", "").replace(" ", "")
        val totalTime = ary(4).trim.toInt
        val inNum = ary(5).trim.toInt
        val outNum = ary(6).trim.toInt
        val inOrgId = ary(7).trim
        val outOrgId = ary(8).trim

        return Some(PassengerMatchRecord(deviceId, startTime, inNum, outNum, inOrgId, outOrgId))
      }
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage)
      }
    }
    return None
  }
}
