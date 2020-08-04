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

/**
  * HF
  * 2020-06-18 10:52
  */
object realTime_entrance_station_tatistics_10minutes_right {
  def main(args: Array[String]): Unit = {
    var (groupId, windowDuration, product_or_test, jobDescribe, yarnMode) = ("", 0, "", "", "")
    if (real_product_or_test == "product") {
      groupId = args(0)
      windowDuration = args(1).toInt
      product_or_test = args(2)
      jobDescribe = args(3)
      yarnMode = "yarn-cluster"
    } else {
      groupId = "JX01" //消费组,测试使用
      windowDuration = 5
      product_or_test = "test"
      jobDescribe = "测试"
      yarnMode = "local[6]"
    }
    println("--------版本-09:16---------")

    //1. 从kafka实时消费数据
    val conf: SparkConf = new SparkConf()
      .setAppName(s"realTime_entrance_station_tatistics,信息:${jobDescribe}")
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
    val ETC_TollEn_BillInfo_DStream_Original = if (real_product_or_test == "test") {
      println(s"测试:${DataBaseConstant.TOPIC_etc_tollenbillinfo_Test}")
      MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_etc_tollenbillinfo_Test, groupId)
    } else {
      println(s"生产:${DataBaseConstant.TOPIC_ETC_TollEn_BillInfo_Product}")
      MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_ETC_TollEn_BillInfo_Product, groupId)

    }

    var ETC_TollEn_BillInfo_WindowsDStream: DStream[String] = ETC_TollEn_BillInfo_DStream_Original
      .map(item => item.value())
      .window(Seconds(windowDuration), Seconds(windowDuration))






    //实时流转化为df

    ETC_TollEn_BillInfo_WindowsDStream.filter(_ != null).foreachRDD { rdd => {

      var EtcTollenBillInfoDS_Original: Dataset[ETCTollEnBillInfo] = rdd.map(value => {
        var result: ETCTollEnBillInfo = null
        result = JSON.parseObject(value, classOf[ETCTollEnBillInfo])
        result.statisWindowsTime = getMinutesPeriod()
        result
      }).toDS
      import org.apache.spark.sql.functions._
      val entranceStationTatistics_resultTmp = EtcTollenBillInfoDS_Original
        .groupBy($"enTollStationId", $"enTollStationId",$"statisWindowsTime")
        .agg(
          //--comment 'etc各个车型'etcTypeCount
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(1), 1).otherwise(0)).as("etc_1_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(2), 1).otherwise(0)).as("etc_2_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(3), 1).otherwise(0)).as("etc_3_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(4), 1).otherwise(0)).as("etc_4_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(11), 1).otherwise(0)).as("etc_11_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(12), 1).otherwise(0)).as("etc_12_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(13), 1).otherwise(0)).as("etc_13_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(14), 1).otherwise(0)).as("etc_14_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(15), 1).otherwise(0)).as("etc_15_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(16), 1).otherwise(0)).as("etc_16_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(21), 1).otherwise(0)).as("etc_21_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(22), 1).otherwise(0)).as("etc_22_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(23), 1).otherwise(0)).as("etc_23_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(24), 1).otherwise(0)).as("etc_24_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(25), 1).otherwise(0)).as("etc_25_VehicleType"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleType").equalTo(26), 1).otherwise(0)).as("etc_26_VehicleType"),

          //--comment 'etc各个车种'etcClassCount(28类型好像文档里的不在了)
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(0), 1).otherwise(0)).as("etc_0_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(8), 1).otherwise(0)).as("etc_8_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(10), 1).otherwise(0)).as("etc_10_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(14), 1).otherwise(0)).as("etc_14_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(21), 1).otherwise(0)).as("etc_21_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(22), 1).otherwise(0)).as("etc_22_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(23), 1).otherwise(0)).as("etc_23_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(24), 1).otherwise(0)).as("etc_24_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(25), 1).otherwise(0)).as("etc_25_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(26), 1).otherwise(0)).as("etc_26_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(27), 1).otherwise(0)).as("etc_27_VehicleClass"),
          sum(when(col("mediatype").equalTo(1) && col("vehicleClass").equalTo(28), 1).otherwise(0)).as("etc_28_VehicleClass"),

          //--comment 'etc成功处理'
          sum(when(col("mediatype").equalTo(1) && col("signstatus").equalTo(1), 1).otherwise(0)).as("etcSuccessCount"),

          //--comment 'etcc失败处理'
          sum(when(col("mediatype").equalTo(1) && col("signstatus").equalTo(2), 1).otherwise(0)).as("etcFailCount"),

          //--comment 'cpc各个车型的'cpcTypeCount
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(1), 1).otherwise(0)).as("cpc_1_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(2), 1).otherwise(0)).as("cpc_2_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(3), 1).otherwise(0)).as("cpc_3_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(4), 1).otherwise(0)).as("cpc_4_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(11), 1).otherwise(0)).as("cpc_11_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(12), 1).otherwise(0)).as("cpc_12_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(13), 1).otherwise(0)).as("cpc_13_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(14), 1).otherwise(0)).as("cpc_14_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(15), 1).otherwise(0)).as("cpc_15_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(16), 1).otherwise(0)).as("cpc_16_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(21), 1).otherwise(0)).as("cpc_21_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(22), 1).otherwise(0)).as("cpc_22_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(23), 1).otherwise(0)).as("cpc_23_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(24), 1).otherwise(0)).as("cpc_24_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(25), 1).otherwise(0)).as("cpc_25_VehicleType"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleType").equalTo(26), 1).otherwise(0)).as("cpc_26_VehicleType"),
          //--comment 'cpc各个车种的'cpcClassCount
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(0), 1).otherwise(0)).as("cpc_0_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(8), 1).otherwise(0)).as("cpc_8_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(10), 1).otherwise(0)).as("cpc_10_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(14), 1).otherwise(0)).as("cpc_14_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(21), 1).otherwise(0)).as("cpc_21_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(22), 1).otherwise(0)).as("cpc_22_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(23), 1).otherwise(0)).as("cpc_23_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(24), 1).otherwise(0)).as("cpc_24_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(25), 1).otherwise(0)).as("cpc_25_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(26), 1).otherwise(0)).as("cpc_26_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(27), 1).otherwise(0)).as("cpc_27_VehicleClass"),
          sum(when(col("mediatype").equalTo(2) && col("vehicleClass").equalTo(28), 1).otherwise(0)).as("cpc_28_VehicleClass"),

          //--comment cpc处理的总量
          sum(when(col("mediatype").equalTo(2), 1).otherwise(0)).as("cpcCount"),


          //--comment 纸券交易量
          sum(when(col("mediatype").equalTo(3) , 1).otherwise(0)).as("paperCount"),
          //总的车流量
          count("*").as("allCarCount")

        )//agg算子结束的)




      //进行concat
      val entranceStationTatistics_result = entranceStationTatistics_resultTmp
        .select(
          col("enTollStationId"), //--comment  '出口站编码(国标)'
          col("enTollStationId"), //--comment '出口收费站名称'
          col("statisWindowsTime").as("statisticDay"), //--comment 当做进行统计生时间
          concat_ws("|", $"etc_1_VehicleType", $"etc_2_VehicleType", $"etc_3_VehicleType", $"etc_4_VehicleType", $"etc_11_VehicleType", $"etc_12_VehicleType", $"etc_13_VehicleType", $"etc_14_VehicleType", $"etc_15_VehicleType", $"etc_16_VehicleType", $"etc_21_VehicleType", $"etc_22_VehicleType", $"etc_23_VehicleType", $"etc_24_VehicleType", $"etc_25_VehicleType", $"etc_26_VehicleType").as("etcTypeCount"), //--comment 'etc各个车型'etcTypeCount

          concat_ws("|", $"etc_0_VehicleClass", $"etc_8_VehicleClass", $"etc_10_VehicleClass", $"etc_14_VehicleClass", $"etc_21_VehicleClass", $"etc_22_VehicleClass", $"etc_23_VehicleClass", $"etc_24_VehicleClass", $"etc_25_VehicleClass", $"etc_26_VehicleClass", $"etc_27_VehicleClass", $"etc_28_VehicleClass").as("etcClassCount"), //--comment 'etc各个车种'etcClassCount

          $"etcSuccessCount", //--comment 'etc成功处理'
          $"etcFailCount", //--comment 'etcetc失败处理'
          concat_ws("|", $"cpc_1_VehicleType", $"cpc_2_VehicleType", $"cpc_3_VehicleType", $"cpc_4_VehicleType", $"cpc_11_VehicleType", $"cpc_12_VehicleType", $"cpc_13_VehicleType", $"cpc_14_VehicleType", $"cpc_15_VehicleType", $"cpc_16_VehicleType", $"cpc_21_VehicleType", $"cpc_22_VehicleType", $"cpc_23_VehicleType", $"cpc_24_VehicleType", $"cpc_25_VehicleType", $"cpc_26_VehicleType").as("cpcTypeCount"), //--comment 'cpc各个车型的
          concat_ws("|", $"cpc_0_VehicleClass", $"cpc_8_VehicleClass", $"cpc_10_VehicleClass", $"cpc_14_VehicleClass", $"cpc_21_VehicleClass", $"cpc_22_VehicleClass", $"cpc_23_VehicleClass", $"cpc_24_VehicleClass", $"cpc_25_VehicleClass", $"cpc_26_VehicleClass", $"cpc_27_VehicleClass", $"cpc_28_VehicleClass").as("cpcClassCount"), //--comment 'cpc各个车种的
          $"cpcCount",  //--comment cpc处理的总量
          $"paperCount" , //--comment 纸券交易量
          $"allCarCount"  //所有的车的总量
        )

      entranceStationTatistics_result.persist()


//      val CurrentDate = getCurrentDate()
//
//      println(s"-----聚合搞完(${CurrentDate})--------")
//
//      exitStationTatistics_result.printSchema()

//      exitStationTatistics_result.foreach(x => {
//        println(s"=================")
//        println(s"--结果:${x}")
//        println(s"--x的长度是:${x.size}")
//        println(s"--x的第个是:${x.getAs[String](2)}")
//        println(s"=================")
//      })
//
//      println(s"-----写入mysql--------")

      val resultTableName = if (real_product_or_test == "product") "sharedb.ads_etc_en_flow_statistics" else "spark_test.ads_etc_enConsumeDailyStatistic"
      entranceStationTatistics_result.foreachPartition(Partition => {
        MysqlSink_all.replaceIntoMysql(Partition, resultTableName, 12)
      })


    }
    } //rdd的结束


    //手动提交偏移量
    MySeniorKafkaUtil.submitKfkaOffset(groupId, ETC_TollEn_BillInfo_DStream_Original)


    ssc.start()
    ssc.awaitTermination()

  }


}

