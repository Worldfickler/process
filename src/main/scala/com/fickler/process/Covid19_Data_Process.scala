package com.fickler.process

import com.alibaba.fastjson.JSON
import com.fickler.bean.{CovidBean, StatisticsDataBean}
import com.fickler.util.BaseJdbcSink
import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}

import java.util
import scala.collection.immutable.StringOps
import scala.collection.mutable

/**
 * @author dell
 * @version 1.0
 *          全国各省市疫情数据实时处理统计分析
 */
object Covid19_Data_Process {

  def main(args: Array[String]): Unit = {

    //1.创建StructuredStreaming执行环境
    //StructuredStreaming支持使用SQL来处理实时流数据，数据抽象和SparkSQL一样，也是DataFrame和DataSet
    //所以这里创建StructuredStreaming并执行环境就直接创建SparkSession即可
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Covid19_Data_Process").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //导入隐式转换方便后续使用
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import scala.collection.JavaConversions._

    //2.连接kafka
    //从kafka接收消息
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
      .option("subscribe", "covid19")
      .load()
    //取出消息中的value
    val jsonStrDS: Dataset[String] = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]
    //    jsonStrDS.writeStream
    //      .format("console")//输出目的地
    //      .outputMode("append")//输出模式，默认就是append表示显示新增行
    //      .trigger(Trigger.ProcessingTime(0))//触发间隔，0表示尽可能快的执行
    //      .option("truncate", false)//表示如果列名过长不进行阶段
    //      .start()
    //      .awaitTermination()

    //3.处理数据
    //将jsonStr转为样例类
    val covidBeanDS: Dataset[CovidBean] = jsonStrDS.map(jsonStr => {
      JSON.parseObject(jsonStr, classOf[CovidBean])
    })
    //分离出省份数据
    val provinceDS: Dataset[CovidBean] = covidBeanDS.filter(_.statisticsData != null)
    //分离出城市数据
    val cityDS: Dataset[CovidBean] = covidBeanDS.filter(_.statisticsData == null)
    //分离出各省份每一条的统计数据
    val statisticsDataDS: Dataset[StatisticsDataBean] = provinceDS.flatMap(p => {
      val jsonStr: StringOps = p.statisticsData
      val list: mutable.Buffer[StatisticsDataBean] = JSON.parseArray(jsonStr, classOf[StatisticsDataBean])
      list.map(s => {
        s.provinceShortName = p.provinceShortName
        s.locationId = p.locationId
        s
      })
    })

    //    statisticsDataDS.writeStream
    //      .format("console") //输出目的地
    //      .outputMode("append") //输出模式，默认就是append表示显示新增行
    //      .trigger(Trigger.ProcessingTime(0)) //触发间隔，0表示尽可能快的执行
    //      .option("truncate", false) //表示如果列名过长不进行截断
    //      .start()
    //      .awaitTermination()

    //4.统计分析
    //1.全国疫情汇总信息：现有确诊，累计确诊，现有意思，累计治愈，累计死亡--注意：按照日期分组统计
    val result1: DataFrame = provinceDS
      .groupBy('datetime)
      .agg(sum('currentConfirmedCount) as "currentConfirmedCount", //现有确诊
        sum('confirmedCount) as "confirmedCount", //累计确诊
        sum('suspectedCount) as "suspectedCount", //现有疑似
        sum('curedCount) as "curedCount", //累计治愈
        sum('deadCount) as "deadCount" //累计死亡
      )


    //2.全国各省份累计确诊数地图--注意：按照日期-省份分组
    val result2: DataFrame = provinceDS.select('datetime, 'locationId, 'provinceShortName, 'currentConfirmedCount, 'confirmedCount, 'suspectedCount, 'curedCount, 'deadCount)

    //3.全国疫情新增趋势--注意：按照日期分组聚合
    val result3: DataFrame = statisticsDataDS.groupBy('dateId)
      .agg(sum('confirmedIncr) as "confirmedIncr", //新增确诊
        sum('confirmedCount) as "confirmedCount", //累计确诊
        sum('suspectedCount) as "suspectedCount", //累计疑似
        sum('curedCount) as "curedCount", //累计治愈
        sum('deadCount) as "deadCount" //累计死亡
      )

    //4.境外输入排行--注意：按照日期-城市分组聚合
    val result4: Dataset[Row] = cityDS.filter(_.cityName.equals("境外输入")).groupBy('datetime, 'provinceShortName, 'pid)
      .agg(sum('confirmedCount) as "confirmedCount")
      .sort('confirmedCount.desc)

    //5.统计北京的累计确诊地图
    //......

    //6.结果输出-控制台-mysql
    result1.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    result1.writeStream
      .foreach(new BaseJdbcSink("replace into `covid19_1` (`datetime`, `currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) values (?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, row: Row): Unit = {
          //取出row中的数据
          val datetime: String = row.getAs[String]("datetime")
          val currentConfirmedCount: Long = row.getAs[Long]("currentConfirmedCount")
          val confirmedCount: Long = row.getAs[Long]("confirmedCount")
          val suspectedCount: Long = row.getAs[Long]("suspectedCount")
          val curedCount: Long = row.getAs[Long]("curedCount")
          val deadCount: Long = row.getAs[Long]("deadCount")
          //获取预编译语句对象
          preparedStatement = connection.prepareStatement(sql)
          //给sql设置值
          preparedStatement.setString(1, datetime) //jdbc的索引从1开始
          preparedStatement.setLong(2, currentConfirmedCount)
          preparedStatement.setLong(3, confirmedCount)
          preparedStatement.setLong(4, suspectedCount)
          preparedStatement.setLong(5, curedCount)
          preparedStatement.setLong(6, deadCount)
          preparedStatement.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    result2.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    result2.writeStream
      //在2.2.0版本只能使用foreach,效率较低
      ////在2.4版本有foreachBatch,效率较高
      .foreach(new BaseJdbcSink("REPLACE INTO `covid19_2` (`datetime`, `locationId`, `provinceShortName`, `currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) VALUES (?, ?, ?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, value: Row): Unit = {//注意row的索引从0开始
          val datetime: String = value.getAs[String]("datetime")
          val locationId: Int = value.getAs[Int]("locationId")
          val provinceShortName: String = value.getAs[String]("provinceShortName")
          val currentConfirmedCount: Int = value.getAs[Int]("currentConfirmedCount")
          val confirmedCount: Int = value.getAs[Int]("confirmedCount")
          val suspectedCount: Int = value.getAs[Int]("suspectedCount")
          val curedCount: Int = value.getAs[Int]("curedCount")
          val deadCount: Int = value.getAs[Int]("deadCount")
          //REPLACE表示如果有值则替换,如果没有值则新增(本质上是如果有值则删除再新增,如果没有值则直接新增)
          //注意:在创建表的时候需要指定唯一索引,或者联合主键来判断有没有这个值
          preparedStatement = connection.prepareStatement(sql)
          preparedStatement.setString(1,datetime)//jdbc的索引从1开始
          preparedStatement.setInt(2,locationId)
          preparedStatement.setString(3,provinceShortName)
          preparedStatement.setInt(4,currentConfirmedCount)
          preparedStatement.setInt(5,confirmedCount)
          preparedStatement.setInt(6,suspectedCount)
          preparedStatement.setInt(7,curedCount)
          preparedStatement.setInt(8,deadCount)
          preparedStatement.executeUpdate()
        }
      })
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    result3.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    result3.writeStream
      .foreach(new BaseJdbcSink("REPLACE INTO `covid19_3` (`dateId`, `confirmedIncr`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount`) VALUES (?, ?, ?, ?, ?, ?);") {
        override def realProcess(sql: String, value: Row): Unit = {//注意row的索引从0开始
          val dateId: String = value.getAs[String]("dateId")
          val confirmedIncr: Long = value.getAs[Long]("confirmedIncr")
          val confirmedCount: Long = value.getAs[Long]("confirmedCount")
          val suspectedCount: Long = value.getAs[Long]("suspectedCount")
          val curedCount: Long = value.getAs[Long]("curedCount")
          val deadCount: Long = value.getAs[Long]("deadCount")
          preparedStatement = connection.prepareStatement(sql)
          preparedStatement.setString(1,dateId)//jdbc的索引从1开始
          preparedStatement.setLong(2,confirmedIncr)
          preparedStatement.setLong(3,confirmedCount)
          preparedStatement.setLong(4,suspectedCount)
          preparedStatement.setLong(5,curedCount)
          preparedStatement.setLong(6,deadCount)
          preparedStatement.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()


    result4.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()

    result4.writeStream
      .foreach(new BaseJdbcSink("REPLACE INTO `covid19_4` (`datetime`, `provinceShortName`, `pid`, `confirmedCount`) VALUES (?, ?, ?, ?);") {
        override def realProcess(sql: String, value: Row): Unit = {//注意row的索引从0开始
          val datetime: String = value.getAs[String]("datetime")
          val provinceShortName: String = value.getAs[String]("provinceShortName")
          val pid: Int = value.getAs[Int]("pid")
          val confirmedCount: Long = value.getAs[Long]("confirmedCount")
          preparedStatement = connection.prepareStatement(sql)
          preparedStatement.setString(1,datetime)//jdbc的索引从1开始
          preparedStatement.setString(2,provinceShortName)
          preparedStatement.setInt(3,pid)
          preparedStatement.setLong(4,confirmedCount)
          preparedStatement.executeUpdate()
        }
      })
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", false)
      .start()
      .awaitTermination()

  }

}
