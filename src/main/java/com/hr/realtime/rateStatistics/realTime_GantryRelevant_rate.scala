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

import breeze.linalg.{*, dim}
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

import com.hr.offline.test.test_Constant.GantryInfo
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import com.hr.utils.DataBaseConstant._
import com.hr.utils.DataBaseConstant.{TOPIC_ETC_TollEx_BillInfo_Product, TOPIC_etc_tollexbillinfo_Test}
import com.hr.utils.DataSourceUtil.real_product_or_test
import org.apache.spark.sql.{Dataset, SparkSession}
import spire.syntax.numeric

import scala.util.matching.Regex

/**
  * HF
  * 2020-07-22 9:55
  */
object realTime_GantryRelevant_rate {
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
      subscribe_kafakTopic = "RM_TGHBU_TOPIC"
      //RM_TGHBU_TOPIC(门架心跳流水)
    } else {
      duration_length = 1 //消费组,测试使用
      windowDuration = 5 //秒级,
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[*]"
      kafka_bootstrap_servers = "hadoop103:9092,hadoop104:9092"
      subscribe_kafakTopic = "RM_TGHBU_TOPIC"
    }
    println("--------版本-19:30---------")
    println(s"要写入的mysql数据库是:${Structuredsteaming_2_mysqlDatabaseName}")
    println(s"dim.dim_tb_station_mj_rela")

    val spark: SparkSession = SparkSession
      .builder()
      .master(yarnMode)
      .appName(s"realTime_upload_GantryRelevant_rate:${jobDescribe}")
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
      .option("group.id","hhj_realTime_GantryRelevant_rate")
      //.option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load
      .selectExpr("CAST(topic  AS STRING) as topic ", "CAST(value AS STRING) as value ") //取出kafka的key和value的value
      .as[(String, String)] //读出来是字节流

    println(kafka_bootstrap_servers);
    println(s"-${subscribe_kafakTopic}-");
    structuredSteaming_Original.printSchema()

    var structuredSteaming_cahed_Original = structuredSteaming_Original
      .filter(s"topic = '${subscribe_kafakTopic}' ")
      .map(topic_value => {
        //println(s"主题:${topic_value._1}")
        var etcGantryHeartBeatInfo: EtcGantryHeartBeatInfo = null
        try {
          etcGantryHeartBeatInfo = JSON.parseObject(topic_value._2, classOf[EtcGantryHeartBeatInfo])
          var str = etcGantryHeartBeatInfo.heatVersion
          etcGantryHeartBeatInfo.eventTime = Timestamp.valueOf(s"${str.substring(0,4)}-${str.substring(4,6)}-${str.substring(6,8)} ${str.substring(8,10)}:${str.substring(10,12)}:00")
          var gantryIdList = getALLMachedString("gantryId\":\"\\w*",topic_value._2)
          var RSUStatusList = getALLMachedString("RSUStatus\":\"\\w*",topic_value._2)
          var VPLRStatusList = getALLMachedString("statusCode\":\"\\w*",topic_value._2)
          etcGantryHeartBeatInfo.gantryId =   if (gantryIdList.length == 0) "无数据" else gantryIdList(0).substring(11,gantryIdList(0).length)
          etcGantryHeartBeatInfo.RSUStatus =   if (RSUStatusList.length == 0) null else RSUStatusList(0).substring(12,RSUStatusList(0).length)
          etcGantryHeartBeatInfo.VPLRStatus =   if (VPLRStatusList.length == 0) null else VPLRStatusList(0).substring(13,VPLRStatusList(0).length)
        } catch {
          case e: Exception => e.printStackTrace(); println(e); println("--转换出错--")
        } finally {}
//        println(s"--------------------")
//        println(s"gantryId是:${etcGantryHeartBeatInfo.gantryId}")
//        println(s"RSUStatus是:${etcGantryHeartBeatInfo.RSUStatus}")
      //println(s"VPLRStatus是:${etcGantryHeartBeatInfo.eventTime}")
//        println(s"++++++++++++++")
        etcGantryHeartBeatInfo
      })
      .withWatermark("eventTime", "1 days")


    structuredSteaming_cahed_Original.withColumn("etcGantryHeartBeat_hour_windows", functions.window($"eventTime", "1 hours", "1 hours", "0 seconds")) //15 minutes   0 seconds
      .createOrReplaceTempView("etcGantryHeartBeatInfo_hour_table")

    structuredSteaming_cahed_Original.withColumn("etcGantryHeartBeat_day_windows", functions.window($"eventTime", "24 hours", "24 hours", "16 hours")) //15 minutes   0 seconds
      .filter(" 'RSUStatus' is not null")
      .filter("'VPLRStatus' is not null")
      .filter("gantryId != '无数据' ")
      .createOrReplaceTempView("etcGantryHeartBeatInfo_day_table")


    //读取门架关联表
    var gantryFrame = spark.sql("select stationid,stationname,mjtype,mjid,mjname,tollstation,tollstationhex,roadid,roadname from dim.dim_tb_station_mj_rela")
    gantryFrame.cache()
    //广播门架关联表
    val gantryFrameBroadCast: Broadcast[DataFrame] = spark.sparkContext.broadcast(gantryFrame)
    gantryFrameBroadCast.value.createOrReplaceTempView("tmp_middle_gantry")





    var etcGantryHeartBeatInfo_hour_sql=
      s"""
         |select
         |gantryId ,substring(first(chargeUnitId),1,11) as bl_SubCenter,
         |etcGantryHeartBeat_hour_windows,
         |count(*) as etcGantryHeartBeatInfo_hour_count,
         |to_date(etcGantryHeartBeat_hour_windows.start) as statisDay  ,
         |hour(etcGantryHeartBeat_hour_windows.start) as statisHour  ,
         |getCurrentDateAndTime("1") dataUpdateTime
         |from  etcGantryHeartBeatInfo_hour_table
         |group by gantryId ,substring(gantryId,1,12)  , etcGantryHeartBeat_hour_windows
       """.stripMargin

    var etcGantryHeartBeatInfo_hour_sql_relate=
      s"""
         |select
         |e.gantryId ,t.roadid as roadid,
         |e.etcGantryHeartBeat_hour_windows,
         |count(e.gantryId) as etcGantryHeartBeatInfo_hour_count,
         |to_date(e.etcGantryHeartBeat_hour_windows.start) as statisDay  ,
         |hour(e.etcGantryHeartBeat_hour_windows.start) as statisHour  ,
         |getCurrentDateAndTime("1") dataUpdateTime,
         |t.tollstation as tollstation
         |from  etcGantryHeartBeatInfo_hour_table e
         |left join tmp_middle_gantry t on t.mjid=e.gantryId
         |group by t.roadid ,t.tollstation ,e.gantryId , e.etcGantryHeartBeat_hour_windows
       """.stripMargin


    var etcGantryHeartBeatInfo_day_sql=
      s"""
         |select
         |gantryId,substring(chargeUnitId,1,11) as bl_SubCenter,etcGantryHeartBeat_day_windows,
         |sum(if(instr(rsuStatus, '0') = 0 and rsuStatus != '2',1,0))/sum(if(rsuStatus != '2',1,0)) as rsuStatus_Passrate,
         |sum(if(instr(VPLRStatus, '0') = 0 and VPLRStatus != '2',1,0))/sum(if(VPLRStatus != '2',1,0)) as VPLRStatus_Passrate,
         |to_date(etcGantryHeartBeat_day_windows.start) as statisday  ,
         |getCurrentDateAndTime("1") dataUpdateTime
         |from etcGantryHeartBeatInfo_day_table
         |group by gantryId ,substring(chargeUnitId,1,11) ,etcGantryHeartBeat_day_windows
       """.stripMargin

    var etcGantryHeartBeatInfo_day_sql_relate=
      s"""
         |select
         |e.gantryId ,t.roadid as roadid,
         |etcGantryHeartBeat_day_windows,
         |cast( sum(if(instr(e.rsuStatus, '0') = 0 and e.rsuStatus != '2',1,0))/sum(if(e.rsuStatus != '2',1,0)) as decimal(10,3)) as rsuStatus_Passrate,
         |cast( sum(if((e.VPLRStatus = '0' or e.VPLRStatus='00') and e.VPLRStatus != '2',1,0))/sum(if(e.VPLRStatus != '2',1,0)) as decimal(10,3)) as VPLRStatus_Passrate,
         |to_date(e.etcGantryHeartBeat_day_windows.start)  as statisday  ,
         |getCurrentDateAndTime("1") dataUpdateTime,
         |t.tollstation as tollstation
         |from etcGantryHeartBeatInfo_day_table e
         |left join tmp_middle_gantry t on t.mjid=e.gantryId
         |group by t.roadid ,t.tollstation ,e.gantryId,e.etcGantryHeartBeat_day_windows
       """.stripMargin


    var etcGantryHeartBeatInfo_hour_PassRate_result = spark.sql(etcGantryHeartBeatInfo_hour_sql_relate)

    var etcGantryHeartBeatInfo_day_PassRate_result = spark.sql(etcGantryHeartBeatInfo_day_sql_relate)



    val hour_resultTableName = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_etcGantryHeartBeatInfo_hour_PassRate"
    val hour_PassRate_mysqlSink = new MysqlSink_StructuredStreaming(hour_resultTableName, 8)


    val day_resultTableName = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_etcGantryHeartBeatInfo_day_NormalRate"
    val day_NormalRate_mysqlSink = new MysqlSink_StructuredStreaming(day_resultTableName, 8)


    val hour_PassRate_result = etcGantryHeartBeatInfo_hour_PassRate_result
      .coalesce(1)
      .writeStream
      .outputMode("update")
      //.option("checkpointLocation", "./etcGantryHeartBeatInfo_hour_PassRate_result_StructuredSteaming_checkpoint")
      .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
      .foreach(hour_PassRate_mysqlSink)
      .start


    val day_NormalRate_result = etcGantryHeartBeatInfo_day_PassRate_result
      .coalesce(1)
      .writeStream
      .outputMode("update")
      //.option("checkpointLocation", "./etcGantryHeartBeatInfo_day_PassRate_result_StructuredSteaming_checkpoint")
      .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
      .foreach(day_NormalRate_mysqlSink)
      .start


//        val hour_PassRate_result = etcGantryHeartBeatInfo_hour_PassRate_result.writeStream
//          .format("console")
//          .outputMode("update") //update  ,append ,complete
//          .option("truncate", false) //不省略的显示数据
//          //.option("checkpointLocation", "./StructuredSteaming_checkpoint")
//          .trigger(Trigger.ProcessingTime("1 seconds"))
//          .start


//    val day_PassRate_result = etcGantryHeartBeatInfo_day_PassRate_result.writeStream
//      .format("console")
//      .outputMode("update") //update  ,append ,complete
//      .option("truncate", false) //不省略的显示数据
//      //.option("checkpointLocation", "./StructuredSteaming_checkpoint")
//      .trigger(Trigger.ProcessingTime("1 seconds"))
//      .start



    while (true){
      println(s"--当前时间${getCurrentDate()}--消费情况: "+hour_PassRate_result.lastProgress)
    println(s"--当前时间${getCurrentDate()}--消费情况: "+day_NormalRate_result.lastProgress)
      Thread.sleep(60 * 1000)
    }


    spark.streams.awaitAnyTermination()


  }
}
