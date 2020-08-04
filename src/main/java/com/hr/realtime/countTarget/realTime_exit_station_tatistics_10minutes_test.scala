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
import com.hr.utils.MySeniorKafkaUtil._
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
object realTime_exit_station_tatistics_10minutes_test {

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
    println("--------版本-17:07---------")

    //1. 从kafka实时消费数据
    val conf: SparkConf = new SparkConf()
      .setAppName(s"realTime_exit_station_tatistics,信息:${jobDescribe}")
      .set("spark.streaming.kafka.maxRatePerPartition", "10") //控制每个分区每秒钟最大拉取的数量
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //开启优雅关闭背压机制
      .set("spark.streaming.backpressure.enabled", "true") //开启背压机制
      .set("spark.sql.debug.maxToStringFields", "2000")
      .setMaster(s"${yarnMode}") //local[*],yarn-cluster

    conf.set("spark.driver.allowMultipleContexts","true")
//          val spark = SparkSession.builder()
//            .config(conf)
//            .getOrCreate()
//
//    import spark.implicits._
//    import org.apache.spark.sql.SparkSession._
    val list = List(1, 2, 3, 4, 5,6);

    val sc = new SparkContext(conf);
    val input = sc.parallelize(list)

    input.repartition(5).foreach(value=>{
                val spark = SparkSession.builder()
                  .config(conf)
                  .getOrCreate()

          import spark.implicits._
          import org.apache.spark.sql.SparkSession._
      val exitStationTatisticsSql_noFunction =
        s"""
           |select
           |current_date()
       """.stripMargin
      println("------------spark------------")
      println(spark)
      println(exitStationTatisticsSql_noFunction)
      println("------------spark------------")
      try {
        val re4sult =  spark.sql(exitStationTatisticsSql_noFunction)
        re4sult.collect().foreach(x=>println(s"结果是:${x}"))
      } catch {
        case ex: Exception => println("---执行exitStationTatisticsSql发生异常1---------");printf("the error is: %s\n",ex.getMessage )
      } finally {
        println("--exitStationTatisticsSql---finally1")
      }



    })








  }


  def get_real_product_or_test(): String = {
    real_product_or_test
  }

}

