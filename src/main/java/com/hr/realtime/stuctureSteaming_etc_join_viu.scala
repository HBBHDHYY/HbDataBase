package com.hr.realtime
import com.hr.utils.ConfigurationManager
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * HF
  * 2020-06-07 13:24
  */
object stuctureSteaming_etc_join_viu {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaSource")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"))
      .option("subscribe", "topic1")  //订阅topic ,  可以订阅多个
      .load
      .selectExpr("cast(value as string)")  //将字节数组转化为字符串,存储的格式是字节数组
      .as[String]     //是转为ds






    //写入kafak
    df.writeStream
      .outputMode("append")
      .format("kafka") //  // 支持 "orc", "json", "csv"
      .option("kafka.bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"))
      .option("topic", "ss0508")
      .option("checkpointLocation", "./ck3") // 必须指定 checkpoint 目录
      .start
      .awaitTermination()









  }

}
