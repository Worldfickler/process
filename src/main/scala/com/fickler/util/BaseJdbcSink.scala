package com.fickler.util

import org.apache.spark.sql.{ForeachWriter, Row}

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
 * @author dell
 * @version 1.0
 */
abstract class BaseJdbcSink(sql: String) extends ForeachWriter[Row] {
  def realProcess(str: String, row: Row)

  var connection: Connection = _
  var preparedStatement: PreparedStatement = _

  //开启连接
  override def open(partitionId: Long, version: Long): Boolean = {
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?serverTimezone=GMT%2B8&characterEncoding=utf-8&useSSL=false", "root", "root")
    true
  }

  //处理数据-将数据存入mysql
  override def process(value: Row): Unit = {
    realProcess(sql, value)
  }

  //关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (preparedStatement != null) {
      preparedStatement.close()
    }
  }

}
