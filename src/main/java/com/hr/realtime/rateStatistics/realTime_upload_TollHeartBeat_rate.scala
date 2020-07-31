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
  * 2020-07-16 11:32
  */
object realTime_upload_TollHeartBeat_rate {
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
      kafka_bootstrap_servers = "172.27.44.205:6667,172.27.44.206:6667,172.27.44.207:6667,172.27.44.208:6667,172.27.44.209:6667"
      subscribe_kafakTopic = "RM_LHBU_TOPIC"
    } else {
      duration_length = 1 //消费组,测试使用
      windowDuration = 5 //秒级,
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[*]"
      kafka_bootstrap_servers = "hadoop103:9092,hadoop104:9092"
      subscribe_kafakTopic = "RM_LHBU_TOPIC"
    }
    println("--------版本-11:00---------")
    println(s"要写入的mysql数据库是:${Structuredsteaming_2_mysqlDatabaseName}")

    val spark: SparkSession = SparkSession
      .builder()
      .master(yarnMode)
      .appName(s"realTime_upload_TollHeartBeat_rate:${jobDescribe}")
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



    var structuredSteaming_transed_Original = structuredSteaming_Original
      .filter("topic = 'RM_LHBU_TOPIC' ")
      .map(topic_value => {
        //println(s"主题:${topic_value._1}")
        var etcTollHeartBeatInfo: EtcTollHeartBeatInfo = null
        try {
          etcTollHeartBeatInfo = JSON.parseObject(topic_value._2, classOf[EtcTollHeartBeatInfo])
          var id = etcTollHeartBeatInfo.id
          etcTollHeartBeatInfo.eventTime = Timestamp.valueOf(s"${id.substring(21,25)}-${id.substring(25,27)}-${id.substring(27,29)} ${id.substring(29,31)}:${id.substring(31,33)}:${id.substring(33,35)}")
        } catch {
          case e: Exception => e.printStackTrace(); e.getMessage; println("--转换出错--")
        } finally {}
        //println(s"---心跳时间:${etcTollHeartBeatInfo.tollStationId}")
        etcTollHeartBeatInfo
      })


    var structuredSteaming_hour_windows=structuredSteaming_transed_Original.withWatermark("eventTime", "1 days")  //
      .withColumn("EtcTollHeartBeat_hours_windows", functions.window($"eventTime", "1 hours", "1 hours", "0 seconds")) //15 minutes   0 seconds,16 hours

    var structuredSteaming_day_windows=structuredSteaming_transed_Original.withWatermark("eventTime", "1 days") //
      .withColumn("EtcTollHeartBeat_day_windows", functions.window($"eventTime", "24 hours", "24 hours", "16 hours")) //15 minutes   0 seconds


    structuredSteaming_hour_windows.createOrReplaceTempView("etcTollHeartBeatInfo_hour_table")

    structuredSteaming_day_windows.createOrReplaceTempView("etcTollHeartBeatInfo_day_table")

//spark-sql> select count(distinct(orgid,orgtype,laneid)) from tbl_lanecode ;
    //9814




    var etcTollHeartBeatInfo_hour_sql=
      s"""
         |select
         |tollLaneId ,tollStationId ,substring(tollStationId,1,11) as bl_SubCenter,
         |EtcTollHeartBeat_hours_windows,
         |count(*) as etcTollHeartBeatInfo_hour_count,
         |to_date(EtcTollHeartBeat_hours_windows.start) as statisDay  ,
         |hour(EtcTollHeartBeat_hours_windows.start) as statisHour  ,
         |getCurrentDateAndTime("1") dataUpdateTime
         |from  etcTollHeartBeatInfo_hour_table
         |group by tollLaneId ,tollStationId , EtcTollHeartBeat_hours_windows
       """.stripMargin

    var etcTollHeartBeatInfo_day_sql2=
      s"""
         |select
         |tollStationId ,tollLaneId ,EtcTollHeartBeat_day_windows,
         |sum(if(instr(rsuStatus, '0') = 0 and rsuStatus != '2',1,0))/sum(if(rsuStatus != '2',1,0)) as rsuStatus_Passrate,
         |count(sum(if(instr(rsuStatus, '0') = 0 and rsuStatus != '2',1,0))/sum(if(rsuStatus != '2',1,0))) as t1,
         |sum(if(instr(VPLRStatus, '0') = 0 and VPLRStatus != '2',1,0))/sum(if(VPLRStatus != '2',1,0)) as VPLRStatus_Passrate,
         |sum(if(instr(laneControllerStatus, '0') = 0 and laneControllerStatus != '2',1,0))/sum(if(laneControllerStatus != '2',1,0)) as laneControllerStatus_Passrate
         |from etcTollHeartBeatInfo_day_table
         |group by tollStationId ,tollLaneId ,EtcTollHeartBeat_day_windows
         |having sum(if(instr(rsuStatus, '0') = 0 and rsuStatus != '2',1,0))/sum(if(rsuStatus != '2',1,0)) >=0.8
         |and sum(if(instr(VPLRStatus, '0') = 0 and VPLRStatus != '2',1,0))/sum(if(VPLRStatus != '2',1,0)) >=0.8
         |and sum(if(instr(laneControllerStatus, '0') = 0 and laneControllerStatus != '2',1,0))/sum(if(laneControllerStatus != '2',1,0)) >=0.8
      """.stripMargin


    var etcTollHeartBeatInfo_day_sql=
      s"""
         |select
         |tollLaneId,tollStationId,substring(tollStationId,1,11) as bl_SubCenter,EtcTollHeartBeat_day_windows,
         |cast( sum(if(instr(rsuStatus, '0') = 0 and rsuStatus != '2',1,0))/sum(if(rsuStatus != '2',1,0)) as  decimal(10,3)) as rsuStatus_Passrate,
         |cast( sum(if(instr(VPLRStatus, '0') = 0 and VPLRStatus != '2',1,0))/sum(if(VPLRStatus != '2',1,0)) as  decimal(10,3)) as VPLRStatus_Passrate,
         |cast( sum(if(instr(laneControllerStatus, '0') = 0 and laneControllerStatus != '2',1,0))/sum(if(laneControllerStatus != '2',1,0)) as  decimal(10,3))
         | as laneControllerStatus_Passrate,
         |to_date(EtcTollHeartBeat_day_windows.start) as statisday  ,
         |getCurrentDateAndTime("1") dataUpdateTime
         |from etcTollHeartBeatInfo_day_table
         |group by tollLaneId ,tollStationId ,EtcTollHeartBeat_day_windows
      """.stripMargin




    var etcTollHeartBeatInfo_hour_PassRate_result = spark.sql(etcTollHeartBeatInfo_hour_sql)


    var etcTollHeartBeatInfo_day_NormalRate_result = spark.sql(etcTollHeartBeatInfo_day_sql)




    val hour_resultTableName = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_etcTollHeartBeatInfo_hour_PassRate"
    val hour_PassRate_mysqlSink = new MysqlSink_StructuredStreaming(hour_resultTableName, 8)


    val day_resultTableName = s"${Structuredsteaming_2_mysqlDatabaseName}.ads_etcTollHeartBeatInfo_day_NormalRate"
    val day_NormalRate_mysqlSink = new MysqlSink_StructuredStreaming(day_resultTableName, 9)




        val hour_PassRate_result = etcTollHeartBeatInfo_hour_PassRate_result
          .coalesce(2)
          .writeStream
          .outputMode("update")
          //.option("checkpointLocation", "./etcTollHeartBeatInfo_day_NormalRate_result_StructuredSteaming_checkpoint")
          .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
          .foreach(hour_PassRate_mysqlSink)
          .start



    val day_NormalRate_result = etcTollHeartBeatInfo_day_NormalRate_result
      .coalesce(2)
      .writeStream
      .outputMode("update")
      .option("checkpointLocation", "./etcTollHeartBeatInfo_hour_PassRate_result_StructuredSteaming_checkpoint")
      .trigger(Trigger.ProcessingTime(s"${duration_length} seconds"))
      .foreach(day_NormalRate_mysqlSink)
      .start






//    val hour_PassRate_resultn = etcTollHeartBeatInfo_hour_PassRate_result.writeStream
//      .format("console")
//      .outputMode("update") //update  ,append ,complete
//      .option("truncate", false) //不省略的显示数据
//      //.option("checkpointLocation", "./StructuredSteaming_checkpoint")
//      .trigger(Trigger.ProcessingTime("1 seconds"))
//      .start
//
//
//        val day_NormalRate_result = etcTollHeartBeatInfo_day_NormalRate_result.writeStream
//          .format("console")
//          .outputMode("update") //update  ,append ,complete
//          .option("truncate", false) //不省略的显示数据
//          //.option("checkpointLocation", "./StructuredSteaming_checkpoint")
//          .trigger(Trigger.ProcessingTime("1 seconds"))
//          .start



//分开批量写
//            val day_NormalRate_result = etcTollHeartBeatInfo_day_NormalRate_result
//              .writeStream
//              .outputMode("update") //update  ,append ,complete
//              .foreachBatch((df, batchId) => {  // 当前分区id, 当前批次id,匿名函数
//              if (df.count() != 0) {
//                df.persist()
//                df.groupBy("tollStationId").agg(sum(col("rsuStatus_Passrate"))).show()
//                df.unpersist()
//              }
//            })
//              .trigger(Trigger.ProcessingTime("1 seconds"))
//              .start

    while (true){
      println(s"--当前时间${getCurrentDate()}--消费情况: "+hour_PassRate_result.lastProgress)
      println(s"--当前时间${getCurrentDate()}--消费情况: "+day_NormalRate_result.lastProgress)
      Thread.sleep(60 * 1000)
    }



    spark.streams.awaitAnyTermination()
















  }
}
