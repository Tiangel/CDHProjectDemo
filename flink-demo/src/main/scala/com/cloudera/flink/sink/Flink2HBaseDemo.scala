package com.cloudera.flink.sink

import java.{lang, util}

import com.cloudera.flink.sink.customsink.HBaseSink
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put

/**
 * Flink从Socket读数据，将读取的数据解析后写入HBase示例
 */
object Flink2HBaseDemo {

  val TABLE_NAME = "hbase.table"
  val COLUMN_FAMILY = "family"
  val DEFAULT_HOST_NAME = "localhost"
  val HOST_NAME = "hostname"
  val PORT = "port"

  def main(args: Array[String]): Unit = {

    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    var hostname: String = ""
    var port: Int = 0
    try {
      hostname = parameterTool.get(HOST_NAME, DEFAULT_HOST_NAME)
      port = parameterTool.getInt("port")
    } catch {
      case e: Exception =>
        println("No port specified. Please run 'SocketWindowWordCount --hostname <hostname> --port <port>', where hostname (localhost by default) and port is the address of the text server")
        println("To start a simple text server, run 'netcat -l <port>' and type the input text into the command line")
        return
    }
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketDs: DataStream[String] = streamEnv.socketTextStream(hostname, port)


    val stream: SingleOutputStreamOperator[util.List[Put]] = socketDs.countWindowAll(3).apply(new AllWindowFunction[String, util.List[Put], GlobalWindow]() {
      override def apply(window: GlobalWindow, values: lang.Iterable[String], out: Collector[util.List[Put]]): Unit = {
        val putList = new util.ArrayList[Put]()

        val iterator: util.Iterator[String] = values.iterator

        while (iterator.hasNext) {
          val value: String = iterator.next()
          if (value.length > 0) {
            import org.apache.hadoop.hbase.util.Bytes
            val rowKey: String = value.replace(",", "-")
            val put: Put = new Put(Bytes.toBytes(rowKey))
            put.addColumn(Bytes.toBytes(parameterTool.get(COLUMN_FAMILY)), Bytes.toBytes("name"), Bytes.toBytes(value))
            putList.add(put)
          }
        }
        out.collect(putList)
      }
    })

    stream.addSink(new HBaseSink(parameterTool.get(TABLE_NAME)))

    streamEnv.execute("Flink HBase Sink Demo")
  }
}
