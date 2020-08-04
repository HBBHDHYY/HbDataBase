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
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies}

import java.lang
import java.sql.ResultSet

import com.hr.utils
import com.hr.realtime.countTarget.realTime_exit_station_tatistics_10minutes._
import java.util.Properties

import com.hr.bean.{EtcGantryEtcBillInfo, EtcGantryVehDisDataInfo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.broadcast.Broadcast
import com.hr.utils.definitionFunction._

import scala.collection.mutable

import scala.util.control.Breaks

/**
  * HF
  * 2020-06-18 10:52
  */
object realTime_exit_station_tatistics_10minutes_newKafaComsumer {
  var real_product_or_test = "test" //生产 product ,测试 test

  def main(args: Array[String]): Unit = {
    var (groupId, windowDuration, product_or_test, jobDescribe,yarnMode) =("",0,"","","")
    if (real_product_or_test == "product") {
      groupId = args(0)
      windowDuration = args(1).toInt
      product_or_test = args(2)
      jobDescribe = args(3)
      yarnMode = "yarn-cluster"
    } else {
      groupId = "HB3" //消费组,测试使用
      windowDuration = 5
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[*]"
    }
    println("--------版本-13:07---------")

    //1. 从kafka实时消费数据
    val conf: SparkConf = new SparkConf()
      .setAppName(s"realTime_exit_station_tatistics,信息:${jobDescribe}")
      .set("spark.streaming.kafka.maxRatePerPartition", "10") //控制每个分区每秒钟最大拉取的数量
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //开启优雅关闭背压机制
      .set("spark.streaming.backpressure.enabled", "true") //开启背压机制
      .set("spark.sql.debug.maxToStringFields", "2000")
      .setMaster(s"${yarnMode}") //local[*],yarn-cluster


    conf.set("spark.driver.allowMultipleContexts","true")

    val ssc = new StreamingContext(conf, Seconds(windowDuration)) //Seconds ,Minutes(10)
    val sc = ssc.sparkContext



    import org.apache.kafka.common.serialization.StringDeserializer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> groupId)

    val ETC_TollEx_BillInfo_DStream_Original=KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](List(DataBaseConstant.TOPIC_etc_tollexbillinfo_Test),kafkaParams)
    )


    //从kafka获取车道出口交易数据数据流
//    val ETC_TollEx_BillInfo_DStream_Original = if (real_product_or_test == "test") {
//      println(1)
//      MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_etc_tollexbillinfo_Test, groupId)
//    } else {
//      println(2)
//      MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_ETC_TollEx_BillInfo_Product, groupId)
//
//    }




    var ETC_TollEx_BillInfo_WindowsDStream: DStream[String] = ETC_TollEx_BillInfo_DStream_Original
      .map(item => item.value())
      .window(Seconds(windowDuration), Seconds(windowDuration))



    //    ETC_TollEx_BillInfo_DStream_Original.foreachRDD(rdd=>{
    //      //rdd.foreach(println)
    //      rdd.foreach(item=>{
    //        println("------------")
    //        println(item.key())
    //        println(s"---${item.value()}---")
    //        println("*********")
    //      })
    //    })

    println(3)
    //广播kafka的生产者
    var kafkaProducer = MySeniorKafkaUtil.getAndBroadCastKafkaProducer(ssc)
    val originTableName = "etc_tollexbillinfo" //测试表名
    val resultTableName = "hrdatabasespark.ads_etc_exConsumeDailyStatistic" //测试表名.HrDataBaseSpark.ads_etc_exConsumeDailyStatistic

    println(4);println(DataBaseConstant.TOPIC_ETC_TollEx_BillInfo_Product)
    //实时流转化为df

    ETC_TollEx_BillInfo_WindowsDStream.foreachRDD { rdd => {
      println(5)
      //      val spark = SparkSession.builder()
      //        .config(conf)
      //        .getOrCreate()
      //      import spark.implicits._
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      import org.apache.spark.sql.SparkSession._
      var getCurrentUnixTimestamp = definitionFunction.getCurrentUnixTimestamp()
      spark.udf.register("getMinutesPeriod", (str: String) => getMinutesPeriod())
      println(6)
      var originTableName_df = rdd.map(value => {
        println(s"---${value}---")
        var result: EtcTollexBillInfo = null
        try {
          result = JSON.parseObject(value, classOf[EtcTollexBillInfo])
          println(s"----解析的结果:${result}")
        } catch {
          case ex: Exception => println("-----发生了异常-------")
        } finally {
          println("finally")
        }
        result
      }).toDF()
      originTableName_df.persist()
      println("------------originTableName信息 ------------")
      println("a")
      originTableName_df.foreach(x=>println(s"---每个元素是:${x}"))
      println("b")
      originTableName_df.printSchema()

      println("------------originTableName信息------------")
      originTableName_df.createOrReplaceTempView(s"${originTableName}")
      println(7)

      val exitStationTatisticsSql =
        s"""
           |select
           |extollstationid,--comment  '出口站编码(国标)'
           |exTollStationName ,--comment '出口收费站名称'
           |getMinutesPeriod("1")    statisticDay , --comment 当做进行统计生时间
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
           |from  ${originTableName}
           |group by
           |extollstationid ,
           |exTollStationName,
           |getMinutesPeriod("1")
       """.stripMargin
      println("------------spark------------")
      println(spark)
      println(exitStationTatisticsSql)
      println("------------spark------------")
      try {
        spark.sql(exitStationTatisticsSql)
      } catch {
        case ex: Exception => println("---执行exitStationTatisticsSql发生异常1---------");printf("the error is: %s\n",ex.getMessage )
      } finally {
        println("--exitStationTatisticsSql---finally1")
      }


      try {
        spark.sql(exitStationTatisticsSql).createOrReplaceTempView("exitStationTatistics_tmp")
      } catch {
        case ex: Exception => {
          printf("the error is: %s\n",ex.getMessage )
          printf("the error is: %s\n",ex.getStackTrace )
        }
      } finally {
        println("--exitStationTatisticsSql---finally2")
      }






      //println("----------------exitStationTatisticsSql-------------------")
      //println(exitStationTatisticsSql)
      println(8)
      //进行concat
      val exitStationTatisticsSql_resultSql =
        """
          |select
          |extollstationid,--comment  '出口站编码(国标)'
          |exTollStationName ,--comment '出口收费站名称'
          |statisticDay ,
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
      val exitStationTatistics_result: DataFrame = spark.sql(exitStationTatisticsSql_resultSql)

      //      etc_join_viu.foreachRDD(rdd=>{
      //        rdd.foreach(record=>{
      //          kafkaProducer.value.send(DataBaseConstant.TOPIC_RESULT,record.toString()) //写入resultData主题
      //        })
      //      })

      //      exitStationTatistics_result.foreach(record=>{
      //           kafkaProducer.value.send(DataBaseConstant.TOPIC_etc_tollexbillinfo_result,record.toString()) //写入resultData主题
      //                })


      //      //测试中写
      //            exitStationTatistics_result.write
      //              .format("jdbc")
      //              .option("url", "jdbc:mysql://hadoop102:3306/spark_test?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai&useSSL=false")
      //              .option("user", "root")
      //              .option("password", "123456")
      //              .option("dbtable", "ads_etc_exConsumeDailyStatistic")
      //              .mode(SaveMode.Append)  //Overwrite  Append
      //              .save()

      println(9)
      //生产中写
      exitStationTatistics_result.write
        .format("jdbc")
        .option("url", real_jdbcUrl)
        .option("user", real_jdbcUser)
        .option("password", real_jdbcPassword)
        .option("dbtable", s"${resultTableName}")
        .mode(SaveMode.Append) //Overwrite  Append
        .save()


      println("实时写入")
      exitStationTatistics_result.repartition(5).write.format("json").mode(SaveMode.Append).save("hdfs:///test/hfresult_realtime");

    }}





    println(10)
    //手动提交偏移量
    MySeniorKafkaUtil.submitKfkaOffset(groupId, ETC_TollEx_BillInfo_DStream_Original)


    ssc.start()
    ssc.awaitTermination()

  }


  def get_real_product_or_test(): String = {
    real_product_or_test
  }

}

