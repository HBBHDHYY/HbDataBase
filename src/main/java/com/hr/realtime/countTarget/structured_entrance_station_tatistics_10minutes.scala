package com.hr.realtime.countTarget
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
/**
  * HF
  * 2020-06-28 14:54
  */
import scala.util.matching.Regex
import java.sql.Timestamp
object structured_entrance_station_tatistics_10minutes {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("RealtimeApp")
      .config("spark.debug.maxToStringFields", "250")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  //WARN,INFO
    import spark.implicits._
    val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")
    var etcTollexBillInfo_windowDuration = 5
    // 1. 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
    var EtcTollexBillInfoDS_Original = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop103:9092,hadoop104:9092")
      .option("subscribe", "etc_tollenbillinfo") //订阅的kafka主题
      //.option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load
      .select("value")  //取出kafka的key和value的value
      .as[String]  //读出来是字节流
      .map(value=>{
      var etcTollEnBillInfo = JSON.parseObject(value, classOf[ETCTollEnBillInfo])
      val pattern = new Regex("\"enTime\":\"\\w{4}-\\w{2}-\\w{2}T\\w{2}:\\w{2}:\\w{2}\"")
      val eventTime_Timestamp = (pattern findFirstIn  value).mkString("").substring(10,20) +" "+ (pattern findFirstIn  value).mkString("").substring(21,29)
      etcTollEnBillInfo.eventTime = Timestamp.valueOf(eventTime_Timestamp)
      println(s"---信息:${etcTollEnBillInfo.enTime}")
      println(s"---时间:${etcTollEnBillInfo.eventTime}")
      etcTollEnBillInfo
    })
      .withWatermark("eventTime", "5 second")  //超过2分钟不要



    //when(people("gender") === "male", 0)
    //   *     .when(people("gender") === "female", 1)
    //   *     .otherwise(2)
    // Scala:
    //    *   people.select(when(people("gender") === "male", 0)
    //      *     .when(people("gender") === "female", 1)
    //    *     .otherwise(2))
    //    *
    //    *   // Java:
    //    *   people.select(when(col("gender").equalTo("male"), 0)
    //      *     .when(col("gender").equalTo("female"), 1)
    //    *     .otherwise(2))

    import org.apache.spark.sql.functions._
    val exitStationTatistics_resultTmp = EtcTollexBillInfoDS_Original.groupBy(window($"eventTime", "10 second", "10 second"),$"extollstationid",$"exTollStationName")  //second  minutes hour
      .agg(
      //--comment 'etc各个车型'etcTypeCount
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(1), 1).otherwise(0)).as("etc_1_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(2), 1).otherwise(0)).as("etc_2_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(3), 1).otherwise(0)).as("etc_3_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(4), 1).otherwise(0)).as("etc_4_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(11), 1).otherwise(0)).as("etc_11_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(12), 1).otherwise(0)).as("etc_12_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(13), 1).otherwise(0)).as("etc_13_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(14), 1).otherwise(0)).as("etc_14_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(15), 1).otherwise(0)).as("etc_15_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(16), 1).otherwise(0)).as("etc_16_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(21), 1).otherwise(0)).as("etc_21_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(22), 1).otherwise(0)).as("etc_22_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(23), 1).otherwise(0)).as("etc_23_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(24), 1).otherwise(0)).as("etc_24_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(25), 1).otherwise(0)).as("etc_25_VehicleType"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicletype").equalTo(26), 1).otherwise(0)).as("etc_26_VehicleType"),

      //--comment 'etc各个车种'etcClassCount(28类型好像文档里的不在了)
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(0), 1).otherwise(0)).as("etc_0_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(8), 1).otherwise(0)).as("etc_8_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(10), 1).otherwise(0)).as("etc_10_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(14), 1).otherwise(0)).as("etc_14_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(21), 1).otherwise(0)).as("etc_21_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(22), 1).otherwise(0)).as("etc_22_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(23), 1).otherwise(0)).as("etc_23_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(24), 1).otherwise(0)).as("etc_24_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(25), 1).otherwise(0)).as("etc_25_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(26), 1).otherwise(0)).as("etc_26_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(27), 1).otherwise(0)).as("etc_27_VehicleClass"),
      sum(when(col("mediatype").equalTo(1) && col("exvehicleclass").equalTo(28), 1).otherwise(0)).as("etc_28_VehicleClass"),

      //--comment 'etc成功处理'
      sum(when(col("mediatype").equalTo(1) && col("signstatus").equalTo(1), 1).otherwise(0)).as("etcSuccessCount"),

      //--comment 'etcc失败处理'
      sum(when(col("mediatype").equalTo(1) && col("signstatus").equalTo(2), 1).otherwise(0)).as("etcFailCount"),

      //--comment 'cpc各个车型的'cpcTypeCount
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(1), 1).otherwise(0)).as("cpc_1_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(2), 1).otherwise(0)).as("cpc_2_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(3), 1).otherwise(0)).as("cpc_3_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(4), 1).otherwise(0)).as("cpc_4_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(11), 1).otherwise(0)).as("cpc_11_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(12), 1).otherwise(0)).as("cpc_12_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(13), 1).otherwise(0)).as("cpc_13_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(14), 1).otherwise(0)).as("cpc_14_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(15), 1).otherwise(0)).as("cpc_15_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(16), 1).otherwise(0)).as("cpc_16_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(21), 1).otherwise(0)).as("cpc_21_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(22), 1).otherwise(0)).as("cpc_22_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(23), 1).otherwise(0)).as("cpc_23_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(24), 1).otherwise(0)).as("cpc_24_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(25), 1).otherwise(0)).as("cpc_25_VehicleType"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicletype").equalTo(26), 1).otherwise(0)).as("cpc_26_VehicleType"),
      //--comment 'cpc各个车种的'cpcClassCount
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(0), 1).otherwise(0)).as("cpc_0_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(8), 1).otherwise(0)).as("cpc_8_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(10), 1).otherwise(0)).as("cpc_10_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(14), 1).otherwise(0)).as("cpc_14_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(21), 1).otherwise(0)).as("cpc_21_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(22), 1).otherwise(0)).as("cpc_22_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(23), 1).otherwise(0)).as("cpc_23_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(24), 1).otherwise(0)).as("cpc_24_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(25), 1).otherwise(0)).as("cpc_25_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(26), 1).otherwise(0)).as("cpc_26_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(27), 1).otherwise(0)).as("cpc_27_VehicleClass"),
      sum(when(col("mediatype").equalTo(2) && col("exvehicleclass").equalTo(28), 1).otherwise(0)).as("cpc_28_VehicleClass"),

      //--comment cpc处理的总量
      sum(when(col("mediatype").equalTo(2), 1).otherwise(0)).as("cpcCount"),


      //--comment 纸券成功交易量
      sum(when(col("mediatype").equalTo(3) , 1).otherwise(0)).as("paperCount")


    )//agg算子结束的)









    //进行concat
    val exitStationTatistics_result = exitStationTatistics_resultTmp
      .select(
        col("extollstationid"), //--comment  '出口站编码(国标)'
        col("exTollStationName"), //--comment '出口收费站名称'
        lit("2020-06-27 18:05:23").as("statisticDay"), //--comment 当做进行统计生时间
        concat_ws("|", $"etc_1_VehicleType", $"etc_2_VehicleType", $"etc_3_VehicleType", $"etc_4_VehicleType", $"etc_11_VehicleType", $"etc_12_VehicleType", $"etc_13_VehicleType", $"etc_14_VehicleType", $"etc_15_VehicleType", $"etc_16_VehicleType", $"etc_21_VehicleType", $"etc_22_VehicleType", $"etc_23_VehicleType", $"etc_24_VehicleType", $"etc_25_VehicleType", $"etc_26_VehicleType").as("etcTypeCount"), //--comment 'etc各个车型'etcTypeCount

        concat_ws("|", $"etc_0_VehicleClass", $"etc_8_VehicleClass", $"etc_10_VehicleClass", $"etc_14_VehicleClass", $"etc_21_VehicleClass", $"etc_22_VehicleClass", $"etc_23_VehicleClass", $"etc_24_VehicleClass", $"etc_25_VehicleClass", $"etc_26_VehicleClass", $"etc_27_VehicleClass", $"etc_28_VehicleClass").as("etcClassCount"), //--comment 'etc各个车种'etcClassCount

        $"etcSuccessCount", //--comment 'etc成功处理'
        $"etcFailCount", //--comment 'etcetc失败处理'
        concat_ws("|", $"cpc_1_VehicleType", $"cpc_2_VehicleType", $"cpc_3_VehicleType", $"cpc_4_VehicleType", $"cpc_11_VehicleType", $"cpc_12_VehicleType", $"cpc_13_VehicleType", $"cpc_14_VehicleType", $"cpc_15_VehicleType", $"cpc_16_VehicleType", $"cpc_21_VehicleType", $"cpc_22_VehicleType", $"cpc_23_VehicleType", $"cpc_24_VehicleType", $"cpc_25_VehicleType", $"cpc_26_VehicleType").as("cpcTypeCount"), //--comment 'cpc各个车型的
        concat_ws("|", $"cpc_0_VehicleClass", $"cpc_8_VehicleClass", $"cpc_10_VehicleClass", $"cpc_14_VehicleClass", $"cpc_21_VehicleClass", $"cpc_22_VehicleClass", $"cpc_23_VehicleClass", $"cpc_24_VehicleClass", $"cpc_25_VehicleClass", $"cpc_26_VehicleClass", $"cpc_27_VehicleClass", $"cpc_28_VehicleClass").as("cpcClassCount"), //--comment 'cpc各个车种的
        $"cpcSuccessCount",  //--comment cpc处理的总量

        $"cpcSuccessFee", //--comment cpc成功处理的总金额

        $"paperSuccessCount" //--comment 纸券成功交易量
      )





    val resultTableName = if (real_product_or_test == "product") "HrDataBaseSpark.ads_etc_enConsumeDailyStatistic" else "spark_test.ads_etc_enConsumeDailyStatistic"
    val mysqlSink = new MysqlSink_StructuredStreaming(resultTableName,12)

    val query = exitStationTatistics_result.writeStream
      .outputMode("update")
      .foreach(mysqlSink)
      .start
      .awaitTermination()

    //    exitStationTatistics_result.writeStream //writeStream
    //      .format("console")
    //      .outputMode("Update")   //Update  ,Append
    //      .option("truncate", false)  //不省略的显示数据
    //      .trigger(Trigger.Continuous("1 seconds"))
    //      .start
    //      .awaitTermination()


  }
}
