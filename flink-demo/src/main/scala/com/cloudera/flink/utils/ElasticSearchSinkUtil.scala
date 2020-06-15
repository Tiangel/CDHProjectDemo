package com.cloudera.flink.utils

import java.net.{MalformedURLException, URL}
import java.util

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

object ElasticSearchSinkUtil {


  /**
   * es sink
   *
   * @param hosts es hosts
   * @param bulkFlushMaxActions bulk flush size
   * @param parallelism 并行度
   * @param data 数据
   * @param func
   * @tparam T
   */
  def addSink[T](hosts: util.List[HttpHost], bulkFlushMaxActions: Int,
                 parallelism: Int, data: SingleOutputStreamOperator[T],
                 func: ElasticsearchSinkFunction[T]): Unit = {
    val esSinkBuilder = new ElasticsearchSink.Builder[T](hosts, func)
    esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions)
    data.addSink(esSinkBuilder.build).setParallelism(parallelism)
  }

  /**
   * 解析配置文件的 es hosts
   */
  @throws[MalformedURLException]
  def getEsAddresses(hosts: String): Unit = {
    val hostList: Array[String] = hosts.split(",", -1)
    val addresses: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    for (host <- hostList) {
      if (host.startsWith("http")) {
        val url: URL = new URL(host)
        addresses.add(new HttpHost(url.getHost, url.getPort))
      } else {
        val parts: Array[String] = host.split(":", 2)
        if (parts.length > 1) {
          addresses.add(new HttpHost(parts(0), parts(1).toInt))
        } else {
          throw new MalformedURLException("invalid elasticsearch hosts format")
        }
      }
    }
  }

}
