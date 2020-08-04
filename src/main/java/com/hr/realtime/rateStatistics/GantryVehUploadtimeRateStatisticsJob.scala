package com.hr.realtime.rateStatistics
import java.sql.Timestamp

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
import com.hr.bean.{EtcTollHeartBeatInfo, EtcTollexBillInfo, _}
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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import com.hr.utils.DataBaseConstant._
import com.hr.utils.DataBaseConstant.{TOPIC_ETC_TollEx_BillInfo_Product, TOPIC_etc_tollexbillinfo_Test}
import com.hr.utils.DataSourceUtil.real_product_or_test
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * HF
  * 2020-07-24 17:00
  * 门架牌识流水上传及时率统计
  */
object GantryVehUploadtimeRateStatisticsJob {
  def main(args: Array[String]): Unit = {

    var (duration_length, windowDuration, product_or_test, jobDescribe, yarnMode) = (60, 0, "", "", "")
    var delay_time_length = 900 //可以允许的延迟时间,单位秒
    var kafka_bootstrap_servers = ""
    var subscribe_kafakTopic = ""

    if (real_product_or_test == "product") {
      duration_length = args(0).toInt //无意义
      delay_time_length = args(1).toInt
      product_or_test = args(2)
      jobDescribe = args(3)
      yarnMode = "yarn-cluster"
      kafka_bootstrap_servers = ConfigurationManager.getProperty("Product.bootstrap.servers")
      subscribe_kafakTopic = "GBUPLOAD_VIU_TOPIC"
    } else {
      duration_length = 1 //消费组,测试使用
      windowDuration = 5 //秒级,
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[*]"
      kafka_bootstrap_servers = ConfigurationManager.getProperty("Test.bootstrap.servers")
      subscribe_kafakTopic = "GBUPLOAD_VIU_TOPIC"
    }
    println("--------版本-11:00---------")
    println(s"要写入的mysql数据库是:${Structuredsteaming_2_mysqlDatabaseName}")

    val spark: SparkSession = SparkSession
      .builder()
      .master(yarnMode)
      .appName(s"realTime_GantryPicture_rate:${jobDescribe}")
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





    //读取门架关联表,1631个门架
    var gantryFrame = spark.sql("select stationid,stationname,mjtype,mjid,mjname,tollstation,tollstationhex,roadid,roadname from dim.dim_tb_station_mj_rela")
    //广播门架关联表
    val gantryFrameBroadCast: Broadcast[DataFrame] = spark.sparkContext.broadcast(gantryFrame)
    gantryFrameBroadCast.value.createOrReplaceTempView("tmp_middle_gantry")



    var structuredSteaming_cahed_Original = structuredSteaming_Original
      .filter(s"topic = '${subscribe_kafakTopic}' ")
      .map(value => {
        var etcGantryVehDisDataInfo: EtcGantryVehDisDataInfo = null
        try {
          etcGantryVehDisDataInfo = JSON.parseObject(value._2, classOf[EtcGantryVehDisDataInfo])
          etcGantryVehDisDataInfo.eventTime = Timestamp.valueOf(etcGantryVehDisDataInfo.picTime.replace("T", " "))
        } catch {
          case e: Exception => e.printStackTrace(); e.getMessage; println("--转换出错--")
        } finally {

        }
        etcGantryVehDisDataInfo
      })
      .withWatermark("eventTime", "1 days")
      .withColumn("gantryvehWindows", functions.window($"eventTime", "24 hours", "24 hours", "16 hours"))


    structuredSteaming_cahed_Original.createOrReplaceTempView("etcGantryVehDisDataInfo_temp_view")


    var etcGantryVehDisDataInfo_gantry_sql =
      s"""
         |select
         |tmg.roadid as roadid,tmg.tollstation as tollstation,etv.gantryId as gantryid,etv.gantryvehWindows as gantryvehwindows,
         |sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)<${delay_time_length},1,0)) as timelysum,
         |sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)>=${delay_time_length},1,0)) as delaysum,
         |count(etv.gantryId) as totalsum,
         |cast(sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)<${delay_time_length},1,0))/count(etv.gantryId) as decimal(10,3)) as timelyrate,
         |cast(sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)>=${delay_time_length},1,0))/count(etv.gantryId) as decimal(10,3))  as delayrate,
         |to_date(gantryvehwindows.start) as statisDay  ,
         |getCurrentDateAndTime("1") as updatetime
         |from etcGantryVehDisDataInfo_temp_view etv
         |left join tmp_middle_gantry tmg on tmg.mjid=etv.gantryId
         |group by etv.gantryId,etv.gantryvehWindows,tmg.tollstation,tmg.roadid
       """.stripMargin



    var etcGantryVehDisDataInfo_gantry_sql_test =
      s"""
         |select
         |"1111" as roadid,"22222222" as tollstation,etv.gantryId as gantryid,etv.gantryvehWindows as gantryvehwindows,
         |sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)<${delay_time_length},1,0)) as timelysum,
         |sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)>=${delay_time_length},1,0)) as delaysum,
         |count(etv.gantryId) as totalsum,
         |
 |cast(sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)<${delay_time_length},1,0))/count(etv.gantryId) as decimal(10,3)) as timelyrate,
         |cast(sum(if(unix_timestamp(etv.receiveTime)-unix_timestamp(etv.eventTime)>=${delay_time_length},1,0))/count(etv.gantryId) as decimal(10,3)) as delayrate,
         |
 |to_date(gantryvehwindows.start) as statisDay  ,
         |getCurrentDateAndTime("1") as updatetime
         |from etcGantryVehDisDataInfo_temp_view etv
         |group by etv.gantryId,etv.gantryvehWindows
       """.stripMargin


    var etcGantryVehDisDataInfo_gantry_result = spark.sql(etcGantryVehDisDataInfo_gantry_sql)

    val gantry_resultTableName = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_gantry_picture_uploadtimerate_gantry"
    val gantry_mysqlSink = new MysqlSink_StructuredStreaming(gantry_resultTableName, 11)



//    //门架牌识流水
    val lane_station = etcGantryVehDisDataInfo_gantry_result
      .coalesce(1)
      .writeStream
      .outputMode("update")
      .option("checkpointLocation", "./GantryVehUploadtimeRateStatisticsJob_StructuredSteaming_checkpoint")
      .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
      .foreach(gantry_mysqlSink)
      .start




//        val gantry_dealSuccess_result_console = etcGantryVehDisDataInfo_gantry_result.writeStream
//          .format("console")
//          .outputMode("update") //update  ,append ,complete
//          .option("truncate", false) //不省略的显示数据
//          .option("checkpointLocation", "./StructuredSteaming_checkpoint")
//          .trigger(Trigger.ProcessingTime("1 seconds"))
//          .start

    while (true){
      println(s"--当前时间${getCurrentDate()}--消费情况: "+lane_station.lastProgress)
      Thread.sleep(600 * 1000)
    }



    spark.streams.awaitAnyTermination()
  }
}
