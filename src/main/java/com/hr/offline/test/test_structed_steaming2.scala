package com.hr.offline.test
import org.apache.spark.sql.SparkSession
/**
  * HF
  * 2020-06-27 12:29
  */
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
object test_structed_steaming2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaSource")
      .getOrCreate()
    import spark.implicits._

    val df = spark.readStream    //readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop103:9092,hadoop104:9092") //全部都要写上吗
      .option("subscribe", "etc_tollexbillinfo")  //订阅topic ,  可以订阅多个
      .option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load
    println(s"-----")
    df.printSchema()
    println(s"-----")
    // .select("value", "key")  是kafka特有的key,value

    println(df.isStreaming)

    val df1 = df .selectExpr("cast(value as string)")  //将字节数组转化为字符串,存储的格式是字节数组
      .as[String]     //是转为ds



    println("---55555555---")

    df.writeStream //writeStream
      .format("console")
      .outputMode("Append")  //Complete Append Update
      .option("truncate", false)  //不省略的显示数据
      .trigger(Trigger.Continuous("1 seconds"))
      .start
      .awaitTermination()


    //    df.write
    //      .format("console")
    //      .option("truncate", false)
    //      .save()

  }
}



