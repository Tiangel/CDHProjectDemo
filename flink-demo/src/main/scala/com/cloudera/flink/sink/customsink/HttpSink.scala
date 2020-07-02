package com.cloudera.flink.sink.customsink

import java.io.IOException
import java.util
import java.util.ArrayList

import com.cloudera.flink.utils.GsonUtil
import okhttp3._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.util.SerializableObject
import org.slf4j.LoggerFactory


class HttpSink[IN] extends RichSinkFunction[IN] {

  private val LOG = LoggerFactory.getLogger(this.getClass)
  private var JSON: MediaType = _
  private var client: OkHttpClient = _

  private val lock = new SerializableObject

  private var batchSize: Int = 10
  private var maxNumRetries: Int = 3
  private var maxRequestTime: Long = 10000
  private var retries = 0

  private var batchedList: util.List[IN] = _

  private val CONNECTION_RETRY_DELAY = 500
  private var isRunning = true


  private var postUrl: String = _

  def this(postUrl: String, batchSize: Int, maxNumRetries: Int, maxRequestTime: Long) {
    this()
    this.postUrl = postUrl
    this.batchSize = batchSize
    this.maxNumRetries = maxNumRetries
    this.maxRequestTime = maxRequestTime
  }

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    JSON = MediaType.get("application/json; charset=utf-8")
    batchedList = new util.ArrayList[IN]
    try {
      lock synchronized {
        createConnection()
      }
    } catch {
      case e: IOException =>
        throw new IOException("Cannot create OkHttpClient ", e)
    }
  }

  private def createConnection(): Unit = {
    client = new OkHttpClient.Builder()
      .connectionSpecs(util.Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS))
      .build()
  }

  override def close(): Unit = {
    super.close()
    isRunning = false
    lock synchronized {
      lock.notifyAll()
      if (client != null) {
        client.cache().close()
      }
    }
  }


  private def flush(): Unit = {
    val actionsList: util.List[IN] = new ArrayList[IN]
    var toFlushList: util.List[IN] = null
    lock synchronized {
      if (batchedList.isEmpty) {
        return
      }
      toFlushList = batchedList
      batchedList = new util.ArrayList[IN]
    }

    import scala.collection.JavaConversions._
    for (value <- toFlushList) {
      val action: IN = value
      actionsList.add(action)
    }


    try {
      doHttpPostTX(postUrl, GsonUtil.toJson(actionsList))
    } catch {
      case e: IOException => {
        if (maxNumRetries == 0) {
          throw new IOException("Failed to send message. Connection re-tries are not enabled.", e)
        }
        LOG.error("Failed to send message . Trying to reconnect...", e)

        lock synchronized {
          var lastException: IOException = null
          retries = 0
          while (isRunning && (maxNumRetries < 0 || retries < maxNumRetries)) {
            try {
              if (client != null) {
                client.cache().close()
              }
            } catch {
              case ee: IOException => {
                LOG.error("Could not close socket from failed write attempt", ee)
              }
            }
            retries += 1
            try {
              createConnection()
              doHttpPostTX(postUrl,GsonUtil.toJson(actionsList))
              return
            } catch {
              case ee: IOException => {
                lastException = ee
                LOG.error("Re-connect to server and send message failed. Retry time(s): " + retries, ee)
              }
            }
            lock.wait(CONNECTION_RETRY_DELAY)
          }
          if (isRunning) {
            throw new IOException("Failed to send message. Failed after " + retries + " retries.", lastException)
          }
        }
      }
    }
  }


  override def invoke(actions: IN, context: SinkFunction.Context[_]): Unit = {
    var number = 0
    lock synchronized {
      if (null != actions) {
        batchedList.add(actions)
      }
      number = batchedList.size
    }
    if (number == batchSize) {
      flush()
    }
  }


  private def doHttpPostTX(postUrl: String, request: String): Unit = {
    post(postUrl, request)
  }


  @throws[IOException]
  private def post(url: String, json: String): Unit = {
    val body: RequestBody = RequestBody.create(json, JSON)
    val request: Request = new Request.Builder().url(url).post(body).build
    var response: Response = null
    try {
      response = client.newCall(request).execute
      response.body.string
    } catch {
      case e: Exception => {
        LOG.warn(s"Exception: ${e.getMessage}")
        e.printStackTrace()
      }
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }
}

