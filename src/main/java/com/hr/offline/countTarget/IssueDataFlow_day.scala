package com.hr.offline.countTarget

import org.apache.spark.sql.SparkSession

/**
  * HF
  * 2020-06-14 14:27
  */
object IssueDataFlow_day {
  def main(args: Array[String]): Unit = {
    var (year, month, day, hour, day_or_hour, startProgramRemarks) = (args(0), args(1), args(2), args(3), args(4), args(5))

    val spark = SparkSession.builder()
      .master("local[*]") //local[*],yarn-cluster
      .appName(s"粒度:${day_or_hour},备注:${startProgramRemarks} exit_station_tatistics")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    import org.apache.spark.sql.functions._
    import spark.implicits._
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    spark.conf.set("spark.storage.memoryFraction", "0.4")
    //ods.etc_tollexbillinfo
    val originTableName= "spark_test.ETC_c"  //测试表名
    val resultTableName= "spark_test.ETC_c"  //测试表名
    //测试使用hour
    val IssueDataFlow_day_SQL_supplement = if (day_or_hour == "day") s"and `hour`='${hour}'" else ""

var  IssueDataFlow_day_SQL =
  s"""
    |select
    |concat_ws('-',`year`,`month`,`day`)  collectDay,--comment 采集数据的时间
    |OperTime eventDay , --comment  '事件时间'
    |PosID , --comment 办理网点
    |sum(case when CardType =2 and OperType=82 then 1 else 0 end) chargeCardNum , --comment 记账卡发行量
    |sum(case when CardType =1 and OperType=9 then 1 else 0 end) storeCardNum , --comment 储值卡发行量
    |sum(case when CardType =3 and OperType=65 then 1 else 0 end) obuNum , --comment obu发行量
    |from  ${originTableName}
    |where `year`='${year}' and `month`='${month}' and `day`='${day}' ${IssueDataFlow_day_SQL_supplement}
    |group by concat_ws('-',`year`,`month`,`day`),OperTime ,PosID
  """.stripMargin

spark.sql(IssueDataFlow_day_SQL).createOrReplaceTempView("IssueDataFlow_day_result")

    //插入hive表中
    var IssueDataFlow_day_result_2_hiveTable_Sql =
      s"""
         |from IssueDataFlow_day_result
         |INSERT overwrite table ${resultTableName} partition(collectDay,eventDay)
         |select
         |collectDay, --comment 采集数据的时间
         |eventDay, --comment 当做事件发生时间
         |PosID, --comment 办理网点
         |chargeCardNum, --comment 记账卡发行量
         |storeCardNum, --comment 储值卡发行量
         |obuNum --comment obu发行量
       """.stripMargin

    spark.sql(IssueDataFlow_day_result_2_hiveTable_Sql)

  }
}
