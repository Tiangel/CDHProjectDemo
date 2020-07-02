package com.cloudera.flink.sink.customsink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySqlSink extends RichSinkFunction[Student] {
  private var connection: Connection = _
  private var insertPs: PreparedStatement = _
  private var updatePs: PreparedStatement = _

  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    if (updatePs == null) {
      return
    }
    updatePs.setString(1, value.name)
    updatePs.setString(2, value.password)
    updatePs.setInt(3, value.age)
    updatePs.setLong(4, value.id.toLong)

    updatePs.execute()
    if (updatePs.getUpdateCount == 0) {
      if (insertPs == null) {
        return
      }
      insertPs.setLong(1, value.id.toLong)
      insertPs.setString(2, value.name)
      insertPs.setString(3, value.password)
      insertPs.setInt(4, value.age)
      insertPs.execute()
    }
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection()
    val insertSql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);"
    val updateSql = "update Student set name = ?, password= ?, age= ? where id = ?;"
    if (connection != null) {
      insertPs = this.connection.prepareStatement(insertSql)
      updatePs = this.connection.prepareStatement(updateSql)
    }
  }

  override def close(): Unit = {
    super.close()
    //关闭连接和释放资源
    if (insertPs != null) {
      insertPs.close()
    }
    if (updatePs != null) {
      updatePs.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  def getConnection(): Connection = {
    var conn: Connection = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://bigdata-dev-kafka-01:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    conn
  }
}

class Student extends Serializable {
  var id: BigInt = _
  var name: String = _
  var password: String = _
  var age: Int = _

  def this(id: BigInt, name: String, password: String, age: Int) {
    this()
    this.id = id
    this.name = name
    this.password = password
    this.age = age
  }

  override def toString: String = {
    s"Student[id = ${this.id}, name = ${this.name}, password = ${this.password}, age = ${this.age}]"
  }
}

