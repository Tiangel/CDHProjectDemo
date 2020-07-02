package com.cloudera.flink.sink.customsink

import java.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.conf
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
 * HBaseSink
 */
class HBaseSink extends RichSinkFunction[util.List[Put]] {

  private var tableName: String = null
  private var conn: Connection = null

  def this(tableName: String) {
    this()
    this.tableName = tableName
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val config: conf.Configuration = getConfiguration()
    conn = ConnectionFactory.createConnection(config)
  }

  override def close(): Unit = {
    super.close()
    if (conn != null) {
      conn.close()
    }
  }

  override def invoke(putList: util.List[Put], context: SinkFunction.Context[_]): Unit = {
    if (conn != null) {
      val table: Table = conn.getTable(TableName.valueOf(tableName))
      table.put(putList)
    }
  }

  private def getConfiguration(): org.apache.hadoop.conf.Configuration = {
    val configuration: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
    configuration.addResource(new Path("./core-site.xml"))
    configuration.addResource(new Path("./hdfs-site.xml"))
    configuration.addResource(new Path("./hbase-site.xml"))
    configuration
  }
}
