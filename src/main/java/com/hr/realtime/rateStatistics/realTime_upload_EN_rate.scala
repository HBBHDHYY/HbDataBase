package com.hr.realtime.rateStatistics

import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery, Trigger}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import breeze.numerics.log
import com.hr.{bean, utils}
import com.hr.utils._
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import java.util.Properties
import java.text.SimpleDateFormat

import breeze.linalg.*
import com.hr.bean._
import com.hr.utils.definitionFunction._
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Row
import spire.implicits
import org.apache.spark.sql._
import spire.std.unit
import com.hr.utils.DataSourceUtil._
import org.apache.spark.streaming.kafka010._

import scala.util.control.Breaks
import com.hr.bean.EtcTollexBillInfo
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import com.hr.utils.DataBaseConstant._
import com.hr.utils.DataBaseConstant.{TOPIC_ETC_TollEx_BillInfo_Product, TOPIC_etc_tollexbillinfo_Test}
import com.hr.utils.DataSourceUtil._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

/**
  * HF
  * 2020-07-10 10:18
  */
object realTime_upload_EN_rate {
  def main(args: Array[String]): Unit = {

    var (duration_length, windowDuration, product_or_test, jobDescribe, yarnMode) = (60, 0, "", "", "")
    var delay_time_length = 900 //可以允许的延迟时间,单位秒
    var kafka_bootstrap_servers = ""
    var subscribe_kafakTopic = ""

    if (real_product_or_test == "product") {
      duration_length = args(0).toInt  //处理数据的间隔
      delay_time_length = args(1).toInt
      product_or_test = args(2)
      jobDescribe = args(3)
      yarnMode = "yarn-cluster"
      kafka_bootstrap_servers = ConfigurationManager.getProperty("Product.bootstrap.servers")
      subscribe_kafakTopic = "TRC_ENPU_TOPIC"
    } else {
      duration_length = 1 //消费组,测试使用
      windowDuration = 5 //秒级,
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[*]"
      kafka_bootstrap_servers = ConfigurationManager.getProperty("Test.bootstrap.servers")
      subscribe_kafakTopic = "TRC_ENPU_TOPIC"
    }
    println("--------版本-11:00---------")
    println(s"要写入的mysql数据库是:${Structuredsteaming_2_mysqlDatabaseName}")

    val spark: SparkSession = SparkSession
      .builder()
      .master(yarnMode)
      .appName(s"realTime_upload_EX_rate:${jobDescribe}")
      .config("spark.debug.maxToStringFields", "2000")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN") //WARN,INFO
    import spark.implicits._
    spark.udf.register("getCurrentDateAndTime", (str: String) => getCurrentDate())

    // 1. 从 kafka 读取数据, 为了方便后续处理,
    var structuredSteaming_Original = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
      .option("subscribe", subscribe_kafakTopic) //订阅的kafka主题
      .option("failOnDataLoss", "false") //数据丢失之后(topic被删除，或者offset不在可用范围内时)查询是否失败
      //.option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load
      .selectExpr("CAST(topic  AS STRING) as topic ", "CAST(value AS STRING) as value ") //取出kafka的key和value的value
      .as[(String, String)] //读出来是字节流
    println(kafka_bootstrap_servers);
    println(s"-${subscribe_kafakTopic}-");
    structuredSteaming_Original.printSchema()





    var structuredSteaming_cahed_Original = structuredSteaming_Original
      .filter("topic = 'TRC_ENPU_TOPIC' ")
      .map(topic_value => {
        //println(s"主题:${topic_value._1}")
        var etcTollenBillInfo: ETCTollEnBillInfo = null
        try {
          etcTollenBillInfo = JSON.parseObject(topic_value._2, classOf[ETCTollEnBillInfo])
          etcTollenBillInfo.eventTime = Timestamp.valueOf(etcTollenBillInfo.enTime.replace("T", " "))
        } catch {
          case e: Exception => e.printStackTrace(); e.getMessage; println("--转换出错--")
        } finally {}
        //      println(s"---信息:${etcTollenBillInfo.enTime}")
        //        println(s"---时间:${etcTollenBillInfo.eventTime}")
        etcTollenBillInfo
      })
      .withWatermark("eventTime", "1 days") //
      .withColumn("en_windows", functions.window($"eventTime", "24 hours", "24 hours", "16 hours")) //15 minutes   0 seconds




    structuredSteaming_cahed_Original.createOrReplaceTempView("etcTollEnBillInfo_table")





    val etcTollEnBillInfo_sql =
      s"""
         |select
         |enTollLaneId ,enTollStationId,bl_SubCenter,en_windows,
         |sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) <= ${delay_time_length},1,0)) as etcTollEnBillInfo_timely_dataSum,
         |sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) > ${delay_time_length},1,0)) as etcTollEnBillInfo_delay_dataSum,
         |count(*) as etcTollEnBillInfo_dataSum,
         |cast(sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) <= ${delay_time_length},1,0))/count(*) as decimal(10,3))  as etcTollEnBillInfo_timely_rate,
         |cast(sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) > ${delay_time_length},1,0))/count(*) as decimal(10,3))  as etcTollEnBillInfo_delay_rate,
         |to_date(en_windows.start) as statisday  ,
         |getCurrentDateAndTime("1") dataUpdateTime
         |from etcTollEnBillInfo_table
         |group  by enTollLaneId ,enTollStationId,bl_SubCenter,en_windows
       """.stripMargin

    var etcTollEnBillInfo_result = spark.sql(etcTollEnBillInfo_sql)


    val etcTollEnBillInfo = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_etcTollEnBillInfo_dataTimelyRate"

    println(s"要写入的mysql表名:${etcTollEnBillInfo}")
    val etcTollEnBillInfo_mysqlSink = new MysqlSink_StructuredStreaming(etcTollEnBillInfo, 11)




    //下面是入口的率的统计


        var query_mysql = etcTollEnBillInfo_result
                  .coalesce(2)
                  .writeStream
                  .outputMode("update")
                  .option("checkpointLocation", "./realTime_upload_EN_rate_StructuredSteaming_checkpoint")
                  .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
                  .foreach(etcTollEnBillInfo_mysqlSink)
                  .start





//        var query_console = etcTollEnBillInfo_result.writeStream
//          .format("console")
//          .outputMode("update") //update  ,append ,complete
//          .option("truncate", false) //不省略的显示数据
//          //.option("checkpointLocation", "./StructuredSteaming_checkpoint")
//          .trigger(Trigger.ProcessingTime("1 seconds"))
//          .start

    while (true){
      println(s"--当前时间${getCurrentDate()}--消费情况: "+query_mysql.lastProgress)
      Thread.sleep(600 * 1000)
    }

    spark.streams.awaitAnyTermination()
  }
}
