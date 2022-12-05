package com.fickler.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.sql.{DriverManager, PreparedStatement, ResultSet}
import scala.collection.mutable

/**
 * @author dell
 * @version 1.0
 * 用来手动维护偏移量到MySql中的工具类
 */
object OffsetUtils {

  /**
   * 根据参数查询偏移量信息，并封装成map返回
   * @param groupId 消费者组名称
   * @param topic 主题
   * @return 偏移量信息封装成map
   */
  def getOffsetsMap(groupId: String, topic: String): mutable.Map[TopicPartition, Long] = {

    //1.获取连接
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?serverTimezone=GMT%2B8&characterEncoding=utf-8&useSSL=false", "root", "root")

    //2.编写sql
    val sql:String = "select * from t_offset where groupid = ? and topic = ?"

    //3.获取ps
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

    //4.设置参数并执行
    preparedStatement.setString(1, groupId)
    preparedStatement.setString(2, topic)
    val resultSet: ResultSet = preparedStatement.executeQuery()

    //5.获取返回值并封装成map
    val offsetsMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    while (resultSet.next()) {
      val partition: Int = resultSet.getInt("partition")
      val offset: Int = resultSet.getInt("offset")
      offsetsMap += new TopicPartition(topic, partition) -> offset
    }

    //6.关闭资源
    resultSet.close()
    preparedStatement.close()
    connection.close()

    //7.返回Map
    offsetsMap
  }

  /**
   * 将消费者组的偏移量信息存入MySql
   *
   * @param groupId 消费者组名称
   * @param offsets 偏移量信息
   */
  def saveOffsets(groupId: String, offsets: Array[OffsetRange]): Unit  = {

    //1.加载驱动并获取连接
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "root")

    //2.编写sql
    val sql:String = "replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)"

    //3.创建预编译语句对象
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

    //4.设置参数并执行
    for (o <- offsets) {
      preparedStatement.setString(1, o.topic)
      preparedStatement.setInt(2, o.partition)
      preparedStatement.setString(3, groupId)
      preparedStatement.setLong(4, o.untilOffset)
      preparedStatement.executeUpdate()
    }

    //5.关闭资源
    preparedStatement.close()
    connection.close()

  }

}
