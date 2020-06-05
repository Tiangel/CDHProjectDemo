package com.cloudera.flink.java.demo

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkBase, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.{ElasticsearchSink, RestClientFactory}
import org.apache.flink.util.ExceptionUtils
import org.apache.http.HttpHost
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{Requests, RestClientBuilder}
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException

class Elasticsearch6SinkDemo {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val host: String = "bigdata-dev-kafka-01"
    val port: Int = 7777

    val socketDs: DataStream[String] = streamEnv.socketTextStream(host, port)

    val httpHosts: util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
    httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"))

    val esSinkBuilder: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String] {
      def createIndexRequest(element: String): IndexRequest = {
        println("saving data: " + element)
        val json: util.HashMap[String, String] = new util.HashMap[String, String]()
        json.put("data", element)
        Requests.indexRequest.index("my-index").`type`("my-type").source(json)
      }

      override def process(element: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        requestIndexer.add(createIndexRequest(element))
        println("saved successfully")
      }
    })
    //    dataStream.addSink(esSinkBuilder.build())


    // bulk.flush.backoff.enable: 用来表示是否开启重试机制
    esSinkBuilder.setBulkFlushBackoff(true)
    //bulk.flush.backoff.delay: 进行重试的时间间隔
    esSinkBuilder.setBulkFlushBackoffDelay(5000)
    // bulk.flush.backoff.type: 重试策略，有两种：
    // EXPONENTIAL 指数型（表示多次重试之间的时间间隔按照指数方式进行增长）、
    // CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
    esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL)
    //bulk.flush.backoff.retries: 失败重试的次数
    esSinkBuilder.setBulkFlushBackoffRetries(5)
    // bulk.flush.max.actions: 批量写入时的最大写入条数
    esSinkBuilder.setBulkFlushMaxActions(1);
    //bulk.flush.max.size.mb: 批量写入时的最大数据量
    esSinkBuilder.setBulkFlushMaxSizeMb(2)
    //bulk.flush.interval.ms: 批量写入的时间间隔，配置后则会按照该时间间隔严格执行，无视上面的两个批量写入配置
    esSinkBuilder.setBulkFlushInterval(3000)

    // 提供一个RestClientFactory, 在内部创建的REST客户端上添加自定义配置
//    esSinkBuilder.setRestClientFactory(new RestClientFactory {
//      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
//        restClientBuilder.setDefaultHeaders()
//        restClientBuilder.setFailureListener()
//        restClientBuilder.setHttpClientConfigCallback(...)
//        restClientBuilder.setMaxRetryTimeoutMillis(...)
//        restClientBuilder.setPathPrefix(...)
//        restClientBuilder.setRequestConfigCallback()
//      }
//    })

//    esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler {
//      override def onFailure(action: ActionRequest,
//                             failure: Throwable,
//                             restStatusCode: Int,
//                             requestIndexer: RequestIndexer
//                            ): Unit = {
//        if (ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]).isPresent()) {
//          // full queue; re-add document for indexing
//          requestIndexer.add(action)
//        } else if (ExceptionUtils.findThrowable(failure, classOf[EsRejectedExecutionException]).isPresent()) {
//          // malformed document; simply drop request without failing sink
//        } else {
//          // for all other failures, fail the sink
//          // here the failure is simply rethrown, but users can also choose to throw custom exceptions
//          throw failure
//        }
//      }
//    })

    socketDs.addSink(esSinkBuilder.build())
    streamEnv.execute("Elasticsearch Sink Demo")
  }
}
