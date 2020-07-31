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
import com.hr.utils.DataSourceUtil.real_product_or_test
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

/**
  * HF
  * 2020-07-10 10:18
  */
object realTime_upload_EX_rate {
  def main(args: Array[String]): Unit = {

    var (duration_length, windowDuration, product_or_test, jobDescribe, yarnMode) = (60, 0, "", "", "")
    var delay_time_length = 900 //可以允许的延迟时间,单位秒
    var kafka_bootstrap_servers = ""
    var subscribe_kafakTopic = ""

    if (real_product_or_test == "product") {
      duration_length = args(0).toInt //处理数据的间隔
      delay_time_length = args(1).toInt
      product_or_test = args(2)
      jobDescribe = args(3)
      yarnMode = "yarn-cluster"
      kafka_bootstrap_servers = "172.27.44.205:6667,172.27.44.206:6667,172.27.44.207:6667,172.27.44.208:6667,172.27.44.209:6667"
      subscribe_kafakTopic = "TRC_EXETCPU_TOPIC"
    } else {
      duration_length = 1 //消费组,测试使用
      windowDuration = 5 //秒级,
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[*]"
      kafka_bootstrap_servers = "hadoop103:9092,hadoop104:9092"
      subscribe_kafakTopic = "TRC_EXETCPU_TOPIC"
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


    //入口流水表
    //    structuredSteaming_Original.filter("topic = 'TRC_ENPU_TOPIC' ").map(topic_value=>{
    //      var etcTollEnBillInfo = JSON.parseObject(topic_value._2, classOf[ETCTollEnBillInfo])
    //      etcTollEnBillInfo.eventTime = Timestamp.valueOf(etcTollEnBillInfo.enTime.replace("T"," "))
    //      println(s"---信息:${etcTollEnBillInfo.enTime}")
    //      println(s"---时间:${etcTollEnBillInfo.eventTime}")
    //      etcTollEnBillInfo
    //    }).createOrReplaceTempView("etcTollEnBillInfo_table")

    //出口流水表,TRC_EXETCPU_TOPIC,


    var structuredSteaming_cahed_Original = structuredSteaming_Original
      .filter("topic = 'TRC_EXETCPU_TOPIC' ")
      .map(topic_value => {
        //println(s"主题:${topic_value._1}")
        var etcTollexBillInfo: EtcTollexBillInfo = null
        try {
          etcTollexBillInfo = JSON.parseObject(topic_value._2, classOf[EtcTollexBillInfo])
          etcTollexBillInfo.eventTime = Timestamp.valueOf(etcTollexBillInfo.exTime.replace("T", " "))
        } catch {
          case e: Exception => e.printStackTrace(); e.getMessage; println("--转换出错--")
        } finally {}
        //      println(s"---信息:${etcTollexBillInfo.exTime}")
//        println(s"---时间:${etcTollexBillInfo.eventTime}")
//        println(s"---exVlp:${etcTollexBillInfo.exVlp}")
        etcTollexBillInfo
      })
      .withWatermark("eventTime", "1 days") //
      .withColumn("ex_windows", functions.window($"eventTime", "24 hours", "24 hours", "16 hours")) //15 minutes   0 seconds



    structuredSteaming_cahed_Original.createOrReplaceTempView("etcTollExBillInfo_table")


    var etcTollExBillInfo_sql =
      s"""
         |select
         |enTollLaneId,exTollStationId,bl_SubCenter,ex_windows,
         |sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) <= ${delay_time_length},1,0)) as etcTollExBillInfo_timely_dataSum,
         |sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) > ${delay_time_length},1,0)) as etcTollExBillInfo_delay_dataSum,
         |count(*) as etcTollExBillInfo_dataSum,
         |cast(sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) <= ${delay_time_length},1,0))/count(*) as decimal(10,3))  as etcTollExBillInfo_timely_rate,
         |cast(sum(if(unix_timestamp(receivetime)-unix_timestamp(eventTime) > ${delay_time_length},1,0))/count(*) as decimal(10,3))  as etcTollExBillInfo_delay_rate,
         |to_date(ex_windows.start) as statisday  ,
         |getCurrentDateAndTime("1") dataUpdateTime
         |from etcTollExBillInfo_table
         |group  by enTollLaneId,exTollStationId,bl_SubCenter,ex_windows
       """.stripMargin


    var etcTollExBillInfo_result = spark.sql(etcTollExBillInfo_sql)



    val etcTollExBillInfo_resultTableName = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_etcTollExBillInfo_dataTimelyRate"

    val etcTollExBillInfo_mysqlSink = new MysqlSink_StructuredStreaming(etcTollExBillInfo_resultTableName, 11)




    //下面是出口的率的统计

    val query_mysql = etcTollExBillInfo_result
              .coalesce(2)
              .writeStream
              .outputMode("update")
              //.option("checkpointLocation", "./etcTollExBillInfo_station_result_StructuredSteaming_checkpoint")
              .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
              .foreach(etcTollExBillInfo_mysqlSink)
              .start



//    val query_console = etcTollExBillInfo_result.writeStream
//      .format("console")
//      .outputMode("update") //update  ,append ,complete
//      .option("truncate", false) //不省略的显示数据
//      //.option("checkpointLocation", "./StructuredSteaming_checkpoint")
//      .trigger(Trigger.ProcessingTime("1 seconds"))
//      .start





    while (true){
      println(s"--当前时间${getCurrentDate()}--消费情况: "+query_mysql.lastProgress)
      Thread.sleep(60 * 1000)
    }

    spark.streams.awaitAnyTermination()
  }
}
