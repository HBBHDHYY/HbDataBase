package com.hr.utils


import java.lang
import java.sql.ResultSet

import com.hr.utils
import com.hr.realtime.countTarget.realTime_exit_station_tatistics_10minutes._
import java.util.Properties

import com.hr.bean.{EtcGantryEtcBillInfo, EtcGantryVehDisDataInfo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.{Dataset, Row}
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
import com.hr.utils.DataSourceUtil._
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import scala.collection.mutable

/**
  * HF
  * 2020-06-06 23:05
  */
object MySeniorKafkaUtil {

  var kafkaBootstrapBervers =  if (real_product_or_test=="test") ConfigurationManager.getProperty("Test.bootstrap.servers") else ConfigurationManager.getProperty("Product.bootstrap.servers")

  //private val groupid = ConfigurationManager.getProperty("group.id ")  //消费者组
  //获取kafka的数据流
  def getKafkaStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {

    val groupid = groupId //消费者组
    val topics: Array[String] = Array(topic)   //要消费的主题
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> kafkaBootstrapBervers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> real_earliest_or_latest,  //latest ,earliest
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //println(s"---kafkaBootstrapBervers是:${kafkaBootstrapBervers}")
    //println(s"------kafkaMap是:${kafkaMap}")
   // ssc.checkpoint("hdfs:///user/hf/sparkstreaming/checkpoint")
    ssc.checkpoint("./spark_checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection

     if (real_product_or_test=="test") {
       try {
         sqlProxy.executeQuery(client, "select * from spark_test.spark_kafka_offset_manager where groupid=?", Array(groupid), new QueryCallback {
           override def process(rs: ResultSet): Unit = {
             while (rs.next()) {
               val model = new TopicPartition(rs.getString(2), rs.getInt(3))
               val offset = rs.getLong(4)
               offsetMap.put(model, offset)
             }
             rs.close() //关闭游标
           }
         })
       } catch {
         case e: Exception => e.printStackTrace()
       } finally {
         sqlProxy.shutdown(client)
       }
     } else{
       try {
         println(s"---client是:${client}")
         sqlProxy.executeQuery(client, "select * from sharedb.spark_kafka_offset_manager where groupid=?", Array(groupid), new QueryCallback {
           override def process(rs: ResultSet): Unit = {
             while (rs.next()) {
               val model = new TopicPartition(rs.getString(2), rs.getInt(3))
//               println(s"----rs.getString(1)是:${rs.getString(1)}")
//               println(s"----rs.getString(2)是:${rs.getString(2)}")
//               println(s"----rs.getInt(3)是:${rs.getInt(3)}")
//               println(s"----rs.getLong(4)是:${rs.getLong(4)}")
               val offset = rs.getLong(4)
               offsetMap.put(model, offset)
             }
             rs.close() //关闭游标
           }
         })
       } catch {
         case e: Exception => e.printStackTrace()
       } finally {
         sqlProxy.shutdown(client)
       }
     }

//println(s"---offsetMap是:${offsetMap}")
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    stream
  }

  def getKafkaMultiStream(ssc: StreamingContext, topics: Array[String], groupId: String): InputDStream[ConsumerRecord[String, String]] = {

    val groupid = groupId //消费者组
    //val topics   //要消费的主题
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> kafkaBootstrapBervers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> real_earliest_or_latest,  //latest ,earliest
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    //println(s"---kafkaBootstrapBervers是:${kafkaBootstrapBervers}")
    //println(s"------kafkaMap是:${kafkaMap}")
    // ssc.checkpoint("hdfs:///user/hf/sparkstreaming/checkpoint")
    ssc.checkpoint("./spark_checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection

    if (real_product_or_test=="test") {
      try {
        sqlProxy.executeQuery(client, "select * from spark_test.spark_kafka_offset_manager where groupid=?", Array(groupid), new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              val offset = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            rs.close() //关闭游标
          }
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    } else{
      try {
        println(s"---client是:${client}")
        sqlProxy.executeQuery(client, "select * from sharedb.spark_kafka_offset_manager where groupid=?", Array(groupid), new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val model = new TopicPartition(rs.getString(2), rs.getInt(3))
              //               println(s"----rs.getString(1)是:${rs.getString(1)}")
              //               println(s"----rs.getString(2)是:${rs.getString(2)}")
              //               println(s"----rs.getInt(3)是:${rs.getInt(3)}")
              //               println(s"----rs.getLong(4)是:${rs.getLong(4)}")
              val offset = rs.getLong(4)
              offsetMap.put(model, offset)
            }
            rs.close() //关闭游标
          }
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        sqlProxy.shutdown(client)
      }
    }

    //println(s"---offsetMap是:${offsetMap}")
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    stream
  }


//  def getKafkaMultiStructuredStream(ssc: StreamingContext, topics: Array[String], groupId: String): InputDStream[ConsumerRecord[String, String]] = {
//
//
//
//  }









  //获取kafak的Structured数据流
//  def getKafkaStructuredSteaming(spark: SparkSession , topic: String): Dataset[String]={
//    var EtcTollexBillInfoDS_Original: Dataset[String] = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "hadoop103:9092,hadoop104:9092")
//      .option("subscribe", "etc_tollexbillinfo") //订阅的kafka主题
//      //.option("startingOffsets", "earliest")
//      //.option("endingOffsets", "latest")
//      .load
//      .select("value")  //取出kafka的key和value的value
//      .as[String]  //读出来是字节流
//
//    EtcTollexBillInfoDS_Original
//  }
//广播kafka的生产者,有问题暂时不用
  //      worldRDD.foreachPartition(r
  //      })dd=>{
  //    //        rdd.foreach(record=>{
  //    //          kafkaProducer.value.send("lj03",record)
  //    //        })
  def getAndBroadCastKafkaProducer(ssc: StreamingContext): Broadcast[KafkaSink[String, String]]={
    var kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", kafkaBootstrapBervers)
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p
      }
      ssc.sparkContext.broadcast(utils.KafkaSink[String, String](kafkaProducerConfig))
    }
    kafkaProducer
  }




 //提交消费的偏移量,参数是消费组,stream是从kfka读取的原生流
  def submitKfkaOffset(groupId :String ,stream: InputDStream[ConsumerRecord[String, String]]){

    stream.foreachRDD(rdd => {
      val sqlProxy = new SqlProxy()
      val client = DataSourceUtil.getConnection
       if (real_product_or_test=="test") {
         try {
           val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
           for (or <- offsetRanges) { //删除数据，再新增
             sqlProxy.executeUpdate(client, "replace into spark_test.spark_kafka_offset_manager (groupid,topic,`partition`,untilOffset,submitTime,submitTimeUNIX) values(?,?,?,?,?,?)",
               Array(groupId, or.topic, or.partition.toString, or.untilOffset,getCurrentDate(),getCurrentUnixTimestamp()))
           }
         } catch {
           case e: Exception => e.printStackTrace()
         } finally {
           sqlProxy.shutdown(client)
         }
       } else{
         try {
           val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
           for (or <- offsetRanges) { //删除数据，再新增
             sqlProxy.executeUpdate(client, "replace into sharedb.spark_kafka_offset_manager (groupid,topic,`partition`,untilOffset,submitTime,submitTimeUNIX) values(?,?,?,?,?,?)",
               Array(groupId, or.topic, or.partition.toString, or.untilOffset,getCurrentDate(),getCurrentUnixTimestamp()))
           }
         } catch {
           case e: Exception => e.printStackTrace()
         } finally {
           sqlProxy.shutdown(client)
         }
       }
    })

  }

  def main(args: Array[String]): Unit = {
    println(kafkaBootstrapBervers)
    println(real_product_or_test)
    println(real_product_or_test=="test")
  }


}

























