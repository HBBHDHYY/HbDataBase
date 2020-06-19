package com.hr.utils


import java.lang
import java.sql.ResultSet

import com.hr.utils

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

/**
  * HF
  * 2020-06-06 23:05
  */
object MySeniorKafkaUtil {

  //private val groupid = ConfigurationManager.getProperty("group.id ")  //消费者组
  //获取kafka的数据流
  def getKafkaStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {

    val groupid = groupId //消费者组
    val topics = Array(topic)   //要消费的主题
    val kafkaMap: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> ConfigurationManager.getProperty("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

   // ssc.checkpoint("hdfs:///user/hf/sparkstreaming/checkpoint")
    ssc.checkpoint("./spark_checkpoint")
    //查询mysql中是否有偏移量
    val sqlProxy = new SqlProxy()
    val offsetMap = new mutable.HashMap[TopicPartition, Long]()
    val client = DataSourceUtil.getConnection
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
    val stream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.isEmpty) {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap))
    } else {
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaMap, offsetMap))
    }
    stream
  }


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
        p.setProperty("bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"))
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
    })

  }




}

























