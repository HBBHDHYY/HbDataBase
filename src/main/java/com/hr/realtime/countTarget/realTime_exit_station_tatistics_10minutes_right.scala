package com.hr.realtime.countTarget

import java.text.SimpleDateFormat
import java.util
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

import com.hr.bean._
import com.hr.utils.definitionFunction._
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Row
import spire.implicits
import org.apache.spark.sql._
import spire.std.unit
import com.hr.utils.DataSourceUtil._

import scala.util.control.Breaks
import org.apache.spark.sql.catalyst.expressions.CurrentDate

/**
  * HF
  * 2020-06-18 10:52
  */
object realTime_exit_station_tatistics_10minutes_right {
  def main(args: Array[String]): Unit = {
    var (groupId, windowDuration, product_or_test, jobDescribe, yarnMode) = ("", 0, "", "", "")
    if (real_product_or_test == "product") {
      groupId = args(0)
      windowDuration = args(1).toInt
      product_or_test = args(2)
      jobDescribe = args(3)
      yarnMode = "yarn-cluster"
    } else {
      groupId = "JX6" //消费组,测试使用
      windowDuration = 5   //秒级,
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[6]"
    }
    println("--------版本-14:09---------")

    //1. 从kafka实时消费数据
    val conf: SparkConf = new SparkConf()
      .setAppName(s"realTime_exit_station_tatistics,信息:${jobDescribe}")
      //.set("spark.streaming.kafka.maxRatePerPartition", "10") //控制每个分区每秒钟最大拉取的数量
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //开启优雅关闭背压
      .set("spark.streaming.backpressure.enabled", "true") //开启背压机制
      .set("spark.sql.debug.maxToStringFields", "2000")
      .set("spark.driver.allowMultipleContexts", "true")
      .setMaster(s"${yarnMode}") //local[*],yarn-cluster

    real_product_or_test = product_or_test


    val ssc = new StreamingContext(conf, Seconds(windowDuration)) //Seconds ,Minutes(10)

    //    val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    //    import spark.implicits._
    val spark: SparkSession = SparkSession
      .builder()
      .master("yarn-cluster")
      .appName("RealtimeApp")
      .getOrCreate()
    import spark.implicits._
    spark.udf.register("getMinutesPeriod", (str: String) => getMinutesPeriod())


    //从kafka获取车道出口交易数据数据流
    val ETC_TollEx_BillInfo_DStream_Original = if (real_product_or_test == "test") {
      println(s"测试:${DataBaseConstant.TOPIC_etc_tollexbillinfo_Test}")
      MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_etc_tollexbillinfo_Test, groupId)
    } else {
      println(s"生产:${DataBaseConstant.TOPIC_ETC_TollEx_BillInfo_Product}")
      MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_ETC_TollEx_BillInfo_Product, groupId)

    }


    var ETC_TollEx_BillInfo_WindowsDStream: DStream[String] = ETC_TollEx_BillInfo_DStream_Original
      .map(item => {
        //println("-------123----------")
        //println(s"${item.value()}")
        item.value()
      })
      .window(Seconds(windowDuration), Seconds(windowDuration))






    //实时流转化为df

    ETC_TollEx_BillInfo_WindowsDStream.filter(_ != null).foreachRDD { rdd => {
     // println("rdd开始转换df")

      var EtcTollexBillInfoDS_Original = rdd.map(value => {
        var result: EtcTollexBillInfo = null
        try {
          result = JSON.parseObject(value, classOf[EtcTollexBillInfo])
          result.statisWindowsTime = getMinutesPeriod()
        }catch{
         case e: Exception => e.printStackTrace();e.getMessage ;println("---转换出错---")

        }finally{

        }
        result
      }).toDS
      import org.apache.spark.sql.functions._
      getMinutesPeriod()
      val exitStationTatistics_resultTmp = EtcTollexBillInfoDS_Original
        .groupBy($"extollstationid", $"exTollStationName",$"statisWindowsTime")
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

          //--comment 'etc各个车种'etcClassCount
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

          //--comment 'etcetc失败处理'
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


          //--comment cpc成功处理的
          sum(when(col("mediatype").equalTo(2) && col("signstatus").equalTo(1), 1).otherwise(0)).as("cpcSuccessCount"),
          //--comment cpc成功处理的总金额
          sum(when(col("mediatype").equalTo(2) && col("signstatus").equalTo(1) && col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("cpcSuccessFee"),

          //--comment cpc失败处理的
          sum(when(col("mediatype").equalTo(2) && col("signstatus").equalTo(2), 1).otherwise(0)).as("cpcFailCount"),

          //--comment cpc失败处理的总金额
          sum(when(col("mediatype").equalTo(2) && col("signstatus").equalTo(2) && col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("cpcFailFee"),

          //--comment 纸券成功交易量
          sum(when(col("mediatype").equalTo(3) && col("signstatus").equalTo(1), 1).otherwise(0)).as("paperSuccessCount"),

          //--comment纸券成功交易额

          sum(when(col("mediatype").equalTo(3) && col("signstatus").equalTo(1) && col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("pagerIssueSuccessSumFee"),

          //--comment 纸券失败交易量
          sum(when(col("mediatype").equalTo(3) && col("signstatus").equalTo(2), 1).otherwise(0)).as("paperFailCount"),

          //--comment 纸券失败交易额
          sum(when(col("mediatype").equalTo(3) && col("signstatus").equalTo(2) && col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("paperFailFee"),

          //--comment 总车流量
          count("mediatype").as("totalVeCnt"),

          //--comment 总金额,
          sum(col("fee").cast("decimal(10,4)")).as("totalTransFee"),
          //--comment 总优惠金额
          sum(col("discountFee").cast("decimal(10,4)")).as("totalDiscountFee"),

          //--comment ETC总量
          sum(when(col("mediatype").equalTo(1), 1).otherwise(0)).as("etcCnt"),

          //--comment ETC总交易金额
          sum(when(col("mediatype").equalTo(1) && col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("etcTotalTransFee"),

          //--comment ETC总优惠金额
          sum(when(col("mediatype").equalTo(1) && col("discountFee").notEqual(null), col("discountFee").cast("decimal(10,4)")).otherwise(0)).as("etcTotalDiscountFee"),

          //--comment 无通行介质总数
          sum(when(col("mediatype").equalTo(9), 1).otherwise(0)).as("noMediaTypeTotalNum"),

          //--comment M1通行总数
          sum(when(col("mediatype").equalTo(9), 1).otherwise(0)).as("m1PassNum"),

          //--comment 单省交易总数
          sum(when(col("multiProvince").equalTo(0), 1).otherwise(0)).as("singleProTransTotalNum"),

          //--comment 单省交易金额
          sum(when(col("multiProvince").equalTo(0)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("singleProTransTotalFee"),
          //--comment 多省交易总数
          sum(when(col("multiProvince").equalTo(1), 1).otherwise(0)).as("mutilProTransTotalNum"),
          //--comment 多省交易金额
          sum(when(col("multiProvince").equalTo(1)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("mutilProTransTotalFee"),

          //--comment 各个支付类型总个数payTypeTotalNum
          sum(when(col("payType").equalTo(1), 1).otherwise(0)).as("1_payTypeCount"),
          sum(when(col("payType").equalTo(2), 1).otherwise(0)).as("2_payTypeCount"),
          sum(when(col("payType").equalTo(3), 1).otherwise(0)).as("3_payTypeCount"),
          sum(when(col("payType").equalTo(4), 1).otherwise(0)).as("4_payTypeCount"),
          sum(when(col("payType").equalTo(5), 1).otherwise(0)).as("5_payTypeCount"),
          sum(when(col("payType").equalTo(6), 1).otherwise(0)).as("6_payTypeCount"),
          sum(when(col("payType").equalTo(7), 1).otherwise(0)).as("7_payTypeCount"),
          //--comment 各个支付类型总金额payTypeTotalFee
          sum(when(col("payType").equalTo(1)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("1_payTypeSum"),
          sum(when(col("payType").equalTo(2)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("2_payTypeSum"),
          sum(when(col("payType").equalTo(3)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("3_payTypeSum"),
          sum(when(col("payType").equalTo(4)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("4_payTypeSum"),
          sum(when(col("payType").equalTo(5)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("5_payTypeSum"),
          sum(when(col("payType").equalTo(6)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("6_payTypeSum"),
          sum(when(col("payType").equalTo(7)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("7_payTypeSum"),

          //--comment 实际计费方式总个数actualWayTotalNum
          sum(when(col("actualFeeClass").equalTo(1), 1).otherwise(0)).as("1_actualFeeClassCount"),
          sum(when(col("actualFeeClass").equalTo(2), 1).otherwise(0)).as("2_actualFeeClassCount"),
          sum(when(col("actualFeeClass").equalTo(3), 1).otherwise(0)).as("3_actualFeeClassCount"),
          sum(when(col("actualFeeClass").equalTo(4), 1).otherwise(0)).as("4_actualFeeClassCount"),
          sum(when(col("actualFeeClass").equalTo(5), 1).otherwise(0)).as("5_actualFeeClassCount"),
          sum(when(col("actualFeeClass").equalTo(6), 1).otherwise(0)).as("6_actualFeeClassCount"),
          sum(when(col("actualFeeClass").equalTo(11), 1).otherwise(0)).as("11_actualFeeClassCount"),
          //--comment 实际计费方式总金额actualWayTotalFee
          sum(when(col("payType").equalTo(1)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("1_actualFeeClassSum"),
          sum(when(col("payType").equalTo(2)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("2_actualFeeClassSum"),
          sum(when(col("payType").equalTo(3)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("3_actualFeeClassSum"),
          sum(when(col("payType").equalTo(4)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("4_actualFeeClassSum"),
          sum(when(col("payType").equalTo(5)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("5_actualFeeClassSum"),
          sum(when(col("payType").equalTo(6)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("6_actualFeeClassSum"),
          sum(when(col("payType").equalTo(11)&& col("fee").notEqual(null), col("fee").cast("decimal(10,4)")).otherwise(0)).as("11_actualFeeClassSum")

        )
      //exitStationTatistics_resultTmp.printSchema()

      //进行concat
      val exitStationTatistics_result = exitStationTatistics_resultTmp
        .select(
          col("extollstationid"), //--comment  '出口站编码(国标)'
          col("exTollStationName"), //--comment '出口收费站名称'
          col("statisWindowsTime").as("statisticDay"), //--comment 当做进行统计生时间,getMinutesPeriod(),current_timestamp()
          concat_ws("|", $"etc_1_VehicleType", $"etc_2_VehicleType", $"etc_3_VehicleType", $"etc_4_VehicleType", $"etc_11_VehicleType", $"etc_12_VehicleType", $"etc_13_VehicleType", $"etc_14_VehicleType", $"etc_15_VehicleType", $"etc_16_VehicleType", $"etc_21_VehicleType", $"etc_22_VehicleType", $"etc_23_VehicleType", $"etc_24_VehicleType", $"etc_25_VehicleType", $"etc_26_VehicleType").as("etcTypeCount"), //--comment 'etc各个车型'etcTypeCount

          concat_ws("|", $"etc_0_VehicleClass", $"etc_8_VehicleClass", $"etc_10_VehicleClass", $"etc_14_VehicleClass", $"etc_21_VehicleClass", $"etc_22_VehicleClass", $"etc_23_VehicleClass", $"etc_24_VehicleClass", $"etc_25_VehicleClass", $"etc_26_VehicleClass", $"etc_27_VehicleClass", $"etc_28_VehicleClass").as("etcClassCount"), //--comment 'etc各个车种'etcClassCount

          $"etcSuccessCount", //--comment 'etc成功处理'
          $"etcFailCount", //--comment 'etcetc失败处理'
          concat_ws("|", $"cpc_1_VehicleType", $"cpc_2_VehicleType", $"cpc_3_VehicleType", $"cpc_4_VehicleType", $"cpc_11_VehicleType", $"cpc_12_VehicleType", $"cpc_13_VehicleType", $"cpc_14_VehicleType", $"cpc_15_VehicleType", $"cpc_16_VehicleType", $"cpc_21_VehicleType", $"cpc_22_VehicleType", $"cpc_23_VehicleType", $"cpc_24_VehicleType", $"cpc_25_VehicleType", $"cpc_26_VehicleType").as("cpcTypeCount"), //--comment 'cpc各个车型的
          concat_ws("|", $"cpc_0_VehicleClass", $"cpc_8_VehicleClass", $"cpc_10_VehicleClass", $"cpc_14_VehicleClass", $"cpc_21_VehicleClass", $"cpc_22_VehicleClass", $"cpc_23_VehicleClass", $"cpc_24_VehicleClass", $"cpc_25_VehicleClass", $"cpc_26_VehicleClass", $"cpc_27_VehicleClass", $"cpc_28_VehicleClass").as("cpcClassCount"), //--comment 'cpc各个车种的
          $"cpcSuccessCount", //--comment cpc成功处理的
          $"cpcSuccessFee", //--comment cpc成功处理的总金额
          $"cpcFailCount", //--comment cpc失败处理的
          $"cpcFailFee", //--comment cpc失败处理的总金额
          $"paperSuccessCount", //--comment 纸券成功交易量
          $"pagerIssueSuccessSumFee", //--comment纸券成功交易额
          $"paperFailCount", //--comment 纸券失败交易量
          $"paperFailFee", //--comment 纸券失败交易额
          $"totalVeCnt", //--comment 总车流量
          $"totalTransFee", //--comment 总金额
          $"totalDiscountFee", //--comment 总优惠金额
          $"etcCnt", //--comment ETC总量
          $"etcTotalTransFee", //etcTotalTransFee
          $"etcTotalDiscountFee", //--comment ETC总优惠金额
          $"noMediaTypeTotalNum", //--comment 无通行介质总数
          $"m1PassNum", //--comment M1通行总数
          $"singleProTransTotalNum", //--comment 单省交易总数
          $"singleProTransTotalFee", //--comment 单省交易金额
          $"mutilProTransTotalNum", //--comment 多省交易总数
          $"mutilProTransTotalFee", //--comment 多省交易金额
          concat_ws("|", $"1_payTypeCount", $"2_payTypeCount", $"3_payTypeCount", $"4_payTypeCount", $"5_payTypeCount", $"6_payTypeCount", $"7_payTypeCount").as("payTypeTotalNum"), //--comment 各个支付类型总个数
          concat_ws("|", $"1_payTypeSum", $"2_payTypeSum", $"3_payTypeSum", $"4_payTypeSum", $"5_payTypeSum", $"6_payTypeSum", $"7_payTypeSum").as("payTypeTotalFee"), //--comment 各个支付类型总金额
          concat_ws("|", $"1_actualFeeClassCount", $"2_actualFeeClassCount", $"3_actualFeeClassCount", $"4_actualFeeClassCount", $"5_actualFeeClassCount", $"6_actualFeeClassCount", $"11_actualFeeClassCount").as("actualWayTotalNum"), //--comment 实际计费方式总个数
          concat_ws("|", $"1_actualFeeClassSum", $"2_actualFeeClassSum", $"3_actualFeeClassSum", $"4_actualFeeClassSum", $"5_actualFeeClassSum", $"6_actualFeeClassSum", $"11_actualFeeClassSum").as("actualWayTotalFee")  //--comment 实际计费方式总金额
        )

      exitStationTatistics_result.persist()
      //concat_ws("-",).as("aaaaa") , //  $"", //


      //      var a = ETC_TollEx_BillInfo_WindowsDStream.map((value => {
      //        var result: EtcTollexBillInfo = null
      //        result = JSON.parseObject(value, classOf[EtcTollexBillInfo])
      //        result
      //      }))

     val CurrentDate = getCurrentDate()

      println(s"-----聚合搞完(${CurrentDate})--------")
//     exitStationTatistics_result.printSchema()

//      exitStationTatistics_result.foreach(x => {
//        println(s"=================")
//        println(s"--结果:${x}")
//        println(s"--x的长度是:${x.size}")
//        println(s"--x的第个是:${x.getAs[String](2)}")
//        println(s"=================")
//      })

      println(s"-----写入mysql--------")
      //测试中写
      //                  exitStationTatistics_result.write
      //        .format("jdbc")
      //        .option("url", "jdbc:mysql://hadoop102:3306/spark_test?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false")
      //        .option("user", "root")
      //        .option("password", "123456")
      //        .option("dbtable", "ads_etc_exConsumeDailyStatistic")
      //        .mode(SaveMode.Overwrite)  //Overwrite  Append
      //        .save()
      //生产中写
      //            exitStationTatistics_result.write
      //              .format("jdbc")
      //              .option("url", real_jdbcUrl)
      //              .option("user", real_jdbcUser)
      //              .option("password", real_jdbcPassword)
      //              .option("dbtable", s"${resultTableName}")
      //              .mode(SaveMode.Overwrite) //Overwrite  Append
      //              .save()


      val resultTableName = if (real_product_or_test == "product") "sharedb.ads_etc_ex_flow_statistics" else "spark_test.ads_etc_exConsumeDailyStatistic"

      exitStationTatistics_result.foreachPartition(Partition => {
        MysqlSink_all.replaceIntoMysql(Partition, resultTableName, 33)
      })

      exitStationTatistics_result.foreach(x=>{
        println("---现在是结果写入----")
        println(s"--${x}--")
      })

    }
    } //rdd的结束

    println("--开始手动提交偏移量--")
    //手动提交偏移量
    MySeniorKafkaUtil.submitKfkaOffset(groupId, ETC_TollEx_BillInfo_DStream_Original)


    ssc.start()
    ssc.awaitTermination()

  }


}

