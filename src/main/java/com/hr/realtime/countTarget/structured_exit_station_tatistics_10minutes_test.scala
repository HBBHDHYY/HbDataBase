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
  * 2020-06-20 21:10
  */
import scala.util.matching.Regex
import java.sql.Timestamp
object structured_exit_station_tatistics_10minutes_test {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("RealtimeApp")
      .config("spark.debug.maxToStringFields", "2000")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  //WARN,INFO
    import spark.implicits._
    val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")
    var etcTollexBillInfo_windowDuration = 5

    // 1. 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
    var EtcTollexBillInfoDS_Original: Dataset[String] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop103:9092,hadoop104:9092")
      .option("subscribe", "etc_tollexbillinfo") //订阅的kafka主题
      .option("failOnDataLoss", "false")  //数据丢失之后(topic被删除，或者offset不在可用范围内时)查询是否失败
      //.option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load
      .select("value")  //取出kafka的key和value的value
      .as[String]  //读出来是字节流



    var EtcTollexBillInfoDS_OriginalTransform=EtcTollexBillInfoDS_Original.map(value=>{
      var etcTollexBillInfo:EtcTollexBillInfo = null
      try {
        etcTollexBillInfo = JSON.parseObject(value, classOf[EtcTollexBillInfo])
      }catch{
        case e: Exception => e.printStackTrace();e.getMessage ;println("--转换出错--")
      }finally{

      }
      val pattern = new Regex("\"exTime\":\"\\w{4}-\\w{2}-\\w{2}T\\w{2}:\\w{2}:\\w{2}\"")
      val eventTime_Timestamp = (pattern findFirstIn  value).mkString("").substring(10,20) +" "+ (pattern findFirstIn  value).mkString("").substring(21,29)
      etcTollexBillInfo.eventTime = Timestamp.valueOf(eventTime_Timestamp)
      println(s"---信息:${etcTollexBillInfo.enTime}")
      println(s"---时间:${etcTollexBillInfo.eventTime}")
      println(s"---exVlp:${etcTollexBillInfo.exVlp}")
      etcTollexBillInfo
    })
      .withWatermark("eventTime", "1 second")  //超过2分钟不要



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


    //    val exitStationTatistics_result_grouped = EtcTollexBillInfoDS_OriginalTransform.withColumn("ex_windows", functions.window($"eventTime", "10 second", "10 second","0 second")).groupBy($"extollstationid",$"exTollStationName",$"ex_windows")  //second  minutes hour

//    val exitStationTatistics_result_grouped = EtcTollexBillInfoDS_OriginalTransform.groupBy(window($"eventTime", "10 second", "10 second"),$"extollstationid",$"exTollStationName")

        val exitStationTatistics = EtcTollexBillInfoDS_OriginalTransform.withColumn("ex_windows", functions.window($"eventTime", "10 second", "10 second","0 second"))  //second  minutes hour



   exitStationTatistics.createOrReplaceTempView("exitStationTatistics_table")

    val exitStationTatisticsSql_tmp =
      s"""
         |select
         |extollstationid,--comment  '出口站编码(国标)'
         |exTollStationName ,--comment '出口收费站名称'
         |ex_windows ,
         |sum(case when mediatype = 1 and exvehicletype = 1 then 1 else 0 end)  etc_1_VehicleType, --comment 'etc各个车型'etcTypeCount
         |sum(case when mediatype = 1 and exvehicletype = 2 then 1 else 0 end)  etc_2_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 3 then 1 else 0 end)  etc_3_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 4 then 1 else 0 end)  etc_4_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 11 then 1 else 0 end) etc_11_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 12 then 1 else 0 end) etc_12_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 13 then 1 else 0 end) etc_13_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 14 then 1 else 0 end) etc_14_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 15 then 1 else 0 end) etc_15_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 16 then 1 else 0 end) etc_16_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 21 then 1 else 0 end) etc_21_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 22 then 1 else 0 end) etc_22_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 23 then 1 else 0 end) etc_23_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 24 then 1 else 0 end) etc_24_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 25 then 1 else 0 end) etc_25_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 26 then 1 else 0 end) etc_26_VehicleType,
         |
 |sum(case when mediatype = 1 and exvehicleclass = 0 then 1 else 0 end)  etc_0_VehicleClass, --comment 'etc各个车种'etcClassCount
         |sum(case when mediatype = 1 and exvehicleclass = 8 then 1 else 0 end)  etc_8_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 10 then 1 else 0 end) etc_10_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 14 then 1 else 0 end) etc_14_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 21 then 1 else 0 end) etc_21_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 22 then 1 else 0 end) etc_22_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 23 then 1 else 0 end) etc_23_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 24 then 1 else 0 end) etc_24_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 25 then 1 else 0 end) etc_25_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 26 then 1 else 0 end) etc_26_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 27 then 1 else 0 end) etc_27_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 28 then 1 else 0 end) etc_28_VehicleClass,
         |
 |sum(case when mediatype =1 and signstatus = 1 then 1 else 0 end) as etcSuccessCount, --comment 'etc成功处理'
         |sum(case when mediatype =1 and signstatus = 2 then 1 else 0 end) as etcFailCount, --comment 'etcetc失败处理'
         |
 |sum(case when mediatype = 2 and exvehicletype = 1 then 1 else 0 end)  cpc_1_VehicleType,--comment 'cpc各个车型的'cpcTypeCount
         |sum(case when mediatype = 2 and exvehicletype = 2 then 1 else 0 end)  cpc_2_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 3 then 1 else 0 end)  cpc_3_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 4 then 1 else 0 end)  cpc_4_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 11 then 1 else 0 end) cpc_11_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 12 then 1 else 0 end) cpc_12_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 13 then 1 else 0 end) cpc_13_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 14 then 1 else 0 end) cpc_14_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 15 then 1 else 0 end) cpc_15_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 16 then 1 else 0 end) cpc_16_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 21 then 1 else 0 end) cpc_21_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 22 then 1 else 0 end) cpc_22_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 23 then 1 else 0 end) cpc_23_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 24 then 1 else 0 end) cpc_24_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 25 then 1 else 0 end) cpc_25_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 26 then 1 else 0 end) cpc_26_VehicleType,
         |
 |sum(case when mediatype = 2 and exvehicleclass = 0 then 1 else 0 end)  cpc_0_VehicleClass,--comment 'cpc各个车种的'cpcClassCount
         |sum(case when mediatype = 2 and exvehicleclass = 8 then 1 else 0 end)  cpc_8_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 10 then 1 else 0 end) cpc_10_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 14 then 1 else 0 end) cpc_14_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 21 then 1 else 0 end) cpc_21_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 22 then 1 else 0 end) cpc_22_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 23 then 1 else 0 end) cpc_23_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 24 then 1 else 0 end) cpc_24_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 25 then 1 else 0 end) cpc_25_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 26 then 1 else 0 end) cpc_26_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 27 then 1 else 0 end) cpc_27_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 28 then 1 else 0 end) cpc_28_VehicleClass,
         |
 |sum(case when mediatype =2 and signstatus = 1 then 1 else 0 end) as cpcSuccessCount, --comment cpc成功处理的
         |sum(case when mediatype =2 and signstatus = 1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as cpcSuccessFee, --comment cpc成功处理的总金额
         |sum(case when mediatype =2 and signstatus = 2 then 1 else 0 end) as cpcFailCount, --comment cpc失败处理的
         |sum(case when mediatype =2 and signstatus = 2 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as cpcFailFee,--comment cpc失败处理的总金额
         |
 |sum(case when mediatype =3 and signstatus = 1 then 1 else 0 end) as paperSuccessCount, --comment 纸券成功交易量
         |sum(case when mediatype =3 and signstatus = 1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as pagerIssueSuccessSumFee, --comment纸券成功交易额
         |sum(case when mediatype =3 and signstatus = 2 then 1 else 0 end) as paperFailCount ,--comment 纸券失败交易量
         |sum(case when mediatype =3 and signstatus = 2 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as paperFailFee ,--comment 纸券失败交易额
         |count(1) as totalVeCnt ,--comment 总车流量
         |sum(cast(nvl(fee,0) as decimal(10,4))) as totalTransFee ,--comment 总金额,
         |sum(cast(nvl(discountFee,0) as decimal(10,4)))  totalDiscountFee,--comment 总优惠金额
         |sum(case when mediaType=1 then 1 else 0 end) as etcCnt,--comment ETC总量
         |sum(case when mediatype =1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as etcTotalTransFee,--comment ETC总交易金额
         |sum(case when mediaType=1 then cast(nvl(discountFee,0) as decimal(10,4)) else 0 end) as etcTotalDiscountFee,--comment ETC总优惠金额
         |
 |sum(case when mediatype =9 then 1 else 0 end) as noMediaTypeTotalNum,--comment 无通行介质总数
         |sum(case when mediatype =4 then 1 else 0 end) as m1PassNum,--comment M1通行总数
         |sum(case when multiProvince =0 then 1 else 0 end) as singleProTransTotalNum,--comment 单省交易总数
         |sum(case when multiProvince =0 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as singleProTransTotalFee,--comment 单省交易金额
         |sum(case when multiProvince =1 then 1 else 0 end) as mutilProTransTotalNum,--comment 多省交易总数
         |sum(case when multiProvince =1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as mutilProTransTotalFee,--comment 多省交易金额
         |
 |sum(case when payType= 1  then 1 else 0 end)  1_payTypeCount, --comment 各个支付类型总个数payTypeTotalNum
         |sum(case when payType= 2  then 1 else 0 end)  2_payTypeCount,
         |sum(case when payType= 3  then 1 else 0 end)  3_payTypeCount,
         |sum(case when payType= 4  then 1 else 0 end)  4_payTypeCount,
         |sum(case when payType= 5  then 1 else 0 end)  5_payTypeCount,
         |sum(case when payType= 6  then 1 else 0 end)  6_payTypeCount,
         |sum(case when payType= 7  then 1 else 0 end)  7_payTypeCount,
         |
 |sum(case when payType= 1  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  1_payTypeSum, --comment 各个支付类型总金额payTypeTotalFee
         |sum(case when payType= 2  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  2_payTypeSum,
         |sum(case when payType= 3  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  3_payTypeSum,
         |sum(case when payType= 4  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  4_payTypeSum,
         |sum(case when payType= 5  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  5_payTypeSum,
         |sum(case when payType= 6  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  6_payTypeSum,
         |sum(case when payType= 7  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  7_payTypeSum,
         |
 |sum(case when actualFeeClass= 1  then 1 else 0 end)  1_actualFeeClassCount, --comment 实际计费方式总个数actualWayTotalNum
         |sum(case when actualFeeClass= 2  then 1 else 0 end)  2_actualFeeClassCount,
         |sum(case when actualFeeClass= 3  then 1 else 0 end)  3_actualFeeClassCount,
         |sum(case when actualFeeClass= 4  then 1 else 0 end)  4_actualFeeClassCount,
         |sum(case when actualFeeClass= 5  then 1 else 0 end)  5_actualFeeClassCount,
         |sum(case when actualFeeClass= 6  then 1 else 0 end)  6_actualFeeClassCount,
         |sum(case when actualFeeClass= 11  then 1 else 0 end) 11_actualFeeClassCount,
         |
 |sum(case when payType= 1  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  1_actualFeeClassSum, --comment 实际计费方式总金额actualWayTotalFee
         |sum(case when payType= 2  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  2_actualFeeClassSum,
         |sum(case when payType= 3  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  3_actualFeeClassSum,
         |sum(case when payType= 4  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  4_actualFeeClassSum,
         |sum(case when payType= 5  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  5_actualFeeClassSum,
         |sum(case when payType= 6  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  6_actualFeeClassSum,
         |sum(case when payType= 11  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  11_actualFeeClassSum
         |from  exitStationTatistics_table
         |group by
         |extollstationid ,
         |exTollStationName,
         |ex_windows
       """.stripMargin   //可能是太复杂了,所以出不了结果

    val exitStationTatisticsSql_resultSql =
      """
        |select
        |extollstationid,--comment  '出口站编码(国标)'
        |exTollStationName ,--comment '出口收费站名称'
        |collectDay ,
        |eventDay ,
        |
        |concat('|',etc_1_VehicleType, '|',etc_2_VehicleType,'|',etc_3_VehicleType,'|',etc_4_VehicleType,'|',etc_11_VehicleType,'|',etc_12_VehicleType,'|',etc_13_VehicleType,'|',etc_14_VehicleType,'|',etc_15_VehicleType,'|',etc_16_VehicleType,'|',etc_21_VehicleType,'|',etc_22_VehicleType,'|',etc_23_VehicleType,'|',etc_24_VehicleType,'|',etc_25_VehicleType,'|',etc_26_VehicleType,'|') as etcTypeCount,--comment 'etc各个车型'
        |
        |concat('|',etc_0_VehicleClass, '|',etc_8_VehicleClass,'|',etc_10_VehicleClass,'|',etc_14_VehicleClass,'|',etc_21_VehicleClass,'|',etc_22_VehicleClass,'|',etc_23_VehicleClass,'|',etc_24_VehicleClass,'|',etc_25_VehicleClass,'|',etc_26_VehicleClass,'|',etc_27_VehicleClass,'|',etc_28_VehicleClass,'|') as etcClassCount ,--comment 'etc各个车种',
        |
        |etcSuccessCount, --comment 'etc成功处理'
        |etcFailCount, --comment 'etcetc失败处理'
        |
        |concat('|',cpc_1_VehicleType,'|',cpc_2_VehicleType,'|',cpc_3_VehicleType,'|',cpc_4_VehicleType,'|',cpc_11_VehicleType,'|',cpc_12_VehicleType,'|',cpc_13_VehicleType,'|',cpc_14_VehicleType,'|',cpc_15_VehicleType,'|',cpc_16_VehicleType,'|',cpc_21_VehicleType,'|',cpc_22_VehicleType,'|',cpc_23_VehicleType,'|',cpc_24_VehicleType,'|',cpc_25_VehicleType,'|',cpc_26_VehicleType,'|') as cpcTypeCount, --comment 'cpc各个车型的'
        |
        |concat('|',cpc_0_VehicleClass,'|',cpc_8_VehicleClass,'|',cpc_10_VehicleClass,'|',cpc_14_VehicleClass,'|',cpc_21_VehicleClass,'|',cpc_22_VehicleClass,'|',cpc_23_VehicleClass,'|',cpc_24_VehicleClass,'|',cpc_25_VehicleClass,'|',cpc_26_VehicleClass,'|',cpc_27_VehicleClass,'|',cpc_28_VehicleClass,'|') as cpcClassCount ,--comment 'cpc各个车种的'
        |
        |cpcSuccessCount, --comment cpc成功处理的
        |cpcSuccessFee, --comment cpc成功处理的总金额
        |cpcFailCount, --comment cpc失败处理的
        |cpcFailFee,--comment cpc失败处理的总金额
        |paperSuccessCount, --comment 纸券成功交易量
        |pagerIssueSuccessSumFee, --comment纸券成功交易额
        |paperFailCount ,--comment 纸券失败交易量
        |paperFailFee ,--comment 纸券失败交易额
        |totalVeCnt ,--comment 总车流量
        |totalTransFee ,--comment 总金额,
        |totalDiscountFee,--comment 总优惠金额
        |etcCnt,--comment ETC总量
        |etcTotalTransFee,--comment ETC总交易金额
        |etcTotalDiscountFee,--comment ETC总优惠金额
        |noMediaTypeTotalNum , --comment 无通行介质总数
        |m1PassNum , --comment M1通行总数
        |singleProTransTotalNum,--comment 单省交易总数
        |singleProTransTotalFee,--comment 单省交易金额
        |mutilProTransTotalNum,--comment 多省交易总数
        |mutilProTransTotalFee,--comment 多省交易金额
        |
        |concat('|',1_payTypeCount,'|',2_payTypeCount,'|',3_payTypeCount,'|',4_payTypeCount,'|',5_payTypeCount,'|',6_payTypeCount,'|',7_payTypeCount,'|') as payTypeTotalNum ,--comment 各个支付类型总个数
        |
        |concat('|',1_payTypeSum,'|',2_payTypeSum,'|',3_payTypeSum,'|',4_payTypeSum,'|',5_payTypeSum,'|',6_payTypeSum,'|',7_payTypeSum,'|') as payTypeTotalFee  ,--comment 各个支付类型总金额
        |
        |concat('|',1_actualFeeClassCount, '|',2_actualFeeClassCount,'|',3_actualFeeClassCount,'|',4_actualFeeClassCount,'|',5_actualFeeClassCount,'|',6_actualFeeClassCount,'|',11_actualFeeClassCount,'|')  as actualWayTotalNum , --comment 实际计费方式总个数
        |
        |concat('|',1_actualFeeClassSum,'|',2_actualFeeClassSum,'|',3_actualFeeClassSum,'|',4_actualFeeClassSum,'|',5_actualFeeClassSum,'|',6_actualFeeClassSum,'|',11_actualFeeClassSum,'|') as actualWayTotalFee --comment 实际计费方式总金额
        |
        |from
        |exitStationTatistics_tmp
      """.stripMargin

    var sql=
      s"""
         |select
         |count(*),extollstationid,exTollStationName,ex_windows
         |from exitStationTatistics_table
         |group by extollstationid ,exTollStationName,ex_windows
   """.stripMargin



    var exitStationTatistics_result_tmp = spark.sql(exitStationTatisticsSql_tmp).createOrReplaceTempView("exitStationTatistics_tmp_table")



    var exitStationTatistics_result = spark.sql(exitStationTatisticsSql_resultSql)



        exitStationTatistics_result.writeStream //writeStream
      .format("console")
      .outputMode("complete")   //update  ,append ,complete
      .option("truncate", false)  //不省略的显示数据
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .start
      .awaitTermination()


  }
}
