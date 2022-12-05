package com.fickler.process

import com.alibaba.fastjson.{JSON, JSONObject}
import com.fickler.util.OffsetUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, DriverManager, PreparedStatement}
import scala.collection.mutable

/**
 * @author dell
 * @version 1.0
 *          疫情物资数据的实时处理与分析
 */
object Covid19_WZData_Process {

  def main(args: Array[String]): Unit = {

    //1.准备SparkStreaming的开发环境
    val conf: SparkConf = new SparkConf().setAppName("Covid19_WZData_Process").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint(directory = "./sscckp")

    //2.准备kafka的连接参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      elems = "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "group.id" -> "SparkKafka",
      "auto.offset.reset" -> "latest", //偏移量重置位置
      "enable.auto.commit" -> (true: java.lang.Boolean), //是否自动提交偏移量
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )
    val topics: Array[String] = Array("covid19_wz")

    //从mysql中查询出offset:Map[TopicPartition, Long]
    val offsetsMap: mutable.Map[TopicPartition, Long] = OffsetUtils.getOffsetsMap("SparkKafka", "covid19_wz")
    val kafkaDS: InputDStream[ConsumerRecord[String, String]] = if (offsetsMap.size > 0) {
      println("MySql中记录了offsets信息，从offset处开始消费")
      //3.连接kafka获取消息
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetsMap)
      )
    } else {
      println("MySql没有记录offset信息，从latest出开始消费")
      //3.连接kafka获取消息
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }

    //4.实时处理数据
    //    val valueDS: DStream[String] = kafkaDS.map(_.value())
    //    valueDS.print()
    //    {"count":386,"from":"捐赠","name":"一次性橡胶手套/副"}
    //    {"count":937,"from":"需求","name":"N95口罩/个"}
    //    {"count":848,"from":"消耗","name":"医用防护服/套"}
    //    {"count":931,"from":"消耗","name":"电子体温计/个"}
    //    {"count":590,"from":"捐赠","name":"84消毒液/瓶"}
    //    {"count":544,"from":"需求","name":"84消毒液/瓶"}
    //    {"count":494,"from":"采购","name":"N95口罩/个"}
    //    {"count":876,"from":"下拨","name":"医用外科口罩/个"}
    //    {"count":518,"from":"消耗","name":"医用防护服/套"}
    //    {"count":689,"from":"消耗","name":"医用防护服/套"}
    //从kafka中消费的数据是如上述格式的jsonStr，需要解析为json对象
    //目标是将数据转换为如下格式：
    //名称,采购,下拨,捐赠,消耗,需求,库存
    //N95口罩/个,1000,1000,500,-1000,-1000,500
    //(名称,(采购,下拨,捐赠,消耗,需求,库存))
    //对于同一种类型的数据，应该进行聚合，数据进行运算
    val tupleDS: DStream[(String, (Int, Int, Int, Int, Int, Int))] = kafkaDS.map(record => {
      val jsonStr: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonStr)
      val name: String = jsonObj.getString("name")
      val from: String = jsonObj.getString("from")
      val count: Int = jsonObj.getInteger("count")
      //根据物资来源不同，将count记在不同的位置，最终形成统一的格式
      from match {
        case "采购" => (name, (count, 0, 0, 0, 0, count))
        case "下拨" => (name, (0, count, 0, 0, 0, count))
        case "捐赠" => (name, (0, 0, count, 0, 0, count))
        case "消耗" => (name, (0, 0, 0, -count, 0, -count))
        case "需求" => (name, (0, 0, 0, 0, -count, -count))
      }
    })
    tupleDS.print()
    //    (防护目镜/副,(0,0,0,-19,0,-19))
    //    (电子体温计/个,(0,0,0,0,-821,-821))
    //    (防护目镜/副,(0,0,928,0,0,928))
    //    (医用防护服/套,(0,0,321,0,0,321))
    //    (N95口罩/个,(0,895,0,0,0,895))
    //    (医用外科口罩/个,(0,140,0,0,0,140))
    //    (一次性橡胶手套/副,(0,0,0,-923,0,-923))
    //    (N95口罩/个,(0,0,0,0,-828,-828))
    //    (医用外科口罩/个,(155,0,0,0,0,155))
    //    (84消毒液/瓶,(0,574,0,0,0,574))
    //按照key进行聚合
    //定义一个函数，用来将当前批次的数据和历史数据进行聚合
    val updateFunc = (currentValues: Seq[(Int, Int, Int, Int, Int, Int)], historyValue: Option[(Int, Int, Int, Int, Int, Int)]) => {
      //0.定义变量用来接收当前批次数据
      var current_cg: Int = 0
      var current_xb: Int = 0
      var current_jz: Int = 0
      var current_xh: Int = 0
      var current_xq: Int = 0
      var current_kc: Int = 0

      if (currentValues.size > 0) {
        //1.取出当前批次数据
        for (currentValue <- currentValues) {
          current_cg += currentValue._1
          current_xb += currentValue._2
          current_jz += currentValue._3
          current_xh += currentValue._4
          current_xq += currentValue._5
          current_kc += currentValue._6
        }
        //2.取出历史数据
        val history_cg: Int = historyValue.getOrElse(0, 0, 0, 0, 0, 0)._1
        val history_xb: Int = historyValue.getOrElse(0, 0, 0, 0, 0, 0)._2
        val history_jz: Int = historyValue.getOrElse(0, 0, 0, 0, 0, 0)._3
        val history_xh: Int = historyValue.getOrElse(0, 0, 0, 0, 0, 0)._4
        val history_xq: Int = historyValue.getOrElse(0, 0, 0, 0, 0, 0)._5
        val history_kc: Int = historyValue.getOrElse(0, 0, 0, 0, 0, 0)._6

        //3.将当前批次数据和历史数据进行聚合
        val result_cg: Int = current_cg + history_cg
        val result_xb: Int = current_xb + history_xb
        val result_jz: Int = current_jz + history_jz
        val result_xh: Int = current_xh + history_xh
        val result_xq: Int = current_xq + history_xq
        val result_kc: Int = current_kc + history_kc

        //4.将聚合结果进行返回
        Some((result_cg, result_xb, result_jz, result_xh, result_xq, result_kc))

      } else {
        historyValue
      }
    }
    val resultDS: DStream[(String, (Int, Int, Int, Int, Int, Int))] = tupleDS.updateStateByKey(updateFunc)
//    resultDS.print()

    //5.将处理分析的结果存入到mysql
    resultDS.foreachRDD(rdd => {
      rdd.foreachPartition(lines => {
        //1.开启连接
        val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?serverTimezone=GMT%2B8&characterEncoding=utf-8&useSSL=false", "root", "root")
        //2.编写sql并获取ps
        val sql:String = "replace into covid19_wz(name,cg,xb,jz,xh,xq,kc) values(?,?,?,?,?,?,?)"
        val preparedStatement: PreparedStatement = connection.prepareStatement(sql)
        //3.设置参数并执行
        for (line <- lines) {
          preparedStatement.setString(1, line._1)
          preparedStatement.setInt(2, line._2._1)
          preparedStatement.setInt(3, line._2._2)
          preparedStatement.setInt(4, line._2._3)
          preparedStatement.setInt(5, line._2._4)
          preparedStatement.setInt(6, line._2._5)
          preparedStatement.setInt(7, line._2._6)
          preparedStatement.executeUpdate()
        }
        //4.关闭资源
        preparedStatement.close()
        connection.close()
      })
    })

    //6.手动提交偏移量
    //我们要手动提交偏移量，那么就一位置，消费了一批数据就应该提交一次偏移量
    //在SparkStreaming中数据抽象为DStream，DStream的底层其实也就是RDD，也就是每一批次的数据
    //所以接下来我们应该对DStream中的RDD 进行处理
    kafkaDS.foreachRDD(rdd => {
      if (rdd.count() > 0) {
        rdd.foreach(record => println("从Kafka中消费到每一条消息：" + record))
        //使用封装好的API来存放偏移量并提交
        val offsets: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsets) {
          println(s"topic = ${o.topic}, partition = ${o.partition}, formOffset = ${o.fromOffset}, until = ${o.untilOffset}")
        }
        //手动提交偏移量到kafka的默认主体
        //        kafkaDS.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
        OffsetUtils.saveOffsets("SparkKafka", offsets)
      }
    })

    //7.开启SparkStreaming任务并等待结束
    ssc.start()
    ssc.awaitTermination()

  }

}
