package com.hr.realtime

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

import com.hr.bean.{EtcGantryEtcBillInfo, EtcGantryVehDisDataInfo}
import com.hr.utils.definitionFunction._
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql.Row

import scala.util.control.Breaks

/**
  * HF
  * 2020-06-05 11:13
  */
object realTime_etc_join_viu_tellRow_Wrong {
  def main(args: Array[String]): Unit = {

    var etc_windowDuration = 30
    var frontAndBackLength = 200
    var needPrintdetails = "true"    //打印详细信息
    // 1. 从kafka实时消费数据
    val conf = new SparkConf()
      .setAppName("realTime_etc_join_viu_tellRow")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")   //控制每个分区每秒钟拉取的数量
      .set("spark.streaming.stopGracefullyOnShutdown", "true")  //开启背压机制
      .setMaster("local[3]")

    //conf.set("spark.driver.allowMultipleContexts","true");

    val ssc = new StreamingContext(conf, Seconds(10))
    val sc = ssc.sparkContext

    val groupId = "hf2" //消费组,测试使用



    //从kafka获取etc数据流
    val etcDStreamOriginal = MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_ETC, groupId)
    val etcDStream = etcDStreamOriginal.map(item => item.value())

    //从kfka获取viu数据流
    val viuDStreamOriginal = MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_VIU, groupId)
    val viuDStream =  viuDStreamOriginal.map(item => item.value())
    println(s"--------原始数据--------}")
    etcDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"AAA----etc的数据----${x.toString}")))
    viuDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"BBB----viu的数据----${x.toString}")))




    //广播kafka的生产者
    //MySeniorKafkaUtil.broadCastKafkaProducer(ssc)
    //Minutes(60),Seconds(600),//返回的元组第一个是key不用管,第二个是value
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

    //对两个数据流能做开窗处理,保证滑动距离 + 2分钟 <=  etc的窗口大小 ,必须是批次的整数倍
    val etcWindowsGantryidDStream= etcDStream.map { // 对数据格式做调整
      case value => {
        val etcgantryEtcBillInfo = JSON.parseObject(value, classOf[EtcGantryEtcBillInfo])
        etcgantryEtcBillInfo.transtime = etcgantryEtcBillInfo.transtime.replace("T", " ")
        var etcrowTmp = Row(
          etcgantryEtcBillInfo.tradeid,
          etcgantryEtcBillInfo.vehicleplate,
          etcgantryEtcBillInfo.transtime,
          tranTimeToLong(etcgantryEtcBillInfo.transtime),
          etcgantryEtcBillInfo.gantryid,
          getCurrentDate()
        )
        println(s"${etcrowTmp.getAs[String](4)}---etcrowTmp是:${etcrowTmp}")
        (1,"etc")
      }
    }

    //Minutes(10), Minutes(8)      Minutes(12), Minutes(8)
    val viuWindowsGantryidDStream = viuDStream.map {
      case value => {
        val etcGantryVehDisDataInfo = JSON.parseObject(value, classOf[EtcGantryVehDisDataInfo])
        etcGantryVehDisDataInfo.picTime = etcGantryVehDisDataInfo.picTime.replace("T", " ")
        var viurowTmp = Row(
          etcGantryVehDisDataInfo.picId ,
          etcGantryVehDisDataInfo.vehiclePlate,
          etcGantryVehDisDataInfo.picTime,
          tranTimeToLong(etcGantryVehDisDataInfo.picTime),
          etcGantryVehDisDataInfo.gantryId
        )
        println(s"${viurowTmp.getAs[String](4)}---viurowTmp:${viurowTmp}")
        viurowTmp
      }
    }
      .map(t => {
      (t.getAs[String](4), t)
      (1,"viu")
    })

    //原始数据写入kafaka有写特殊暂时不管
    //    etcDStreamOriginal.map(x=>(1,2)).foreachRDD(rdd=>{
    //      rdd.foreach(record=>{
    //        println("----开始西写入kafka---")//record.value().toString()
    //        kafkaProducer.value.send(DataBaseConstant.TOPIC_RESULT,record.toString()) //写入resultData主题
    //      })
    //    })


    println("------中间------")

    etcWindowsGantryidDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"etc--@@@${x}")))
    viuWindowsGantryidDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"viu--###${x}")))
    //广播变量
    println("开始广播")
    val broadcastVar = sc.broadcast(frontAndBackLength)
    println("结束广播")
//.join(viuWindowsGantryidDStream)
  //  etcWindowsGantryidDStream.join(viuWindowsGantryidDStream).foreachRDD(rdd=>rdd.foreach(x=>println(s"--结果--${x._1}")))

     //etcWindowsGantryidDStream.leftOuterJoin(viuWindowsGantryidDStream).foreachRDD(_.foreach(println))


     etcWindowsGantryidDStream.map(x=>{
       println("9999999")
       (1,1)
     }).leftOuterJoin(viuWindowsGantryidDStream).count().map(x=>{
       println("------121212")
       (1,1)
     }).foreachRDD(_.foreach(println))




    println(s"----开始提交偏移量-----")
    //手动提交偏移量
    MySeniorKafkaUtil.submitKfkaOffset(groupId,etcDStreamOriginal)
    MySeniorKafkaUtil.submitKfkaOffset(groupId,viuDStreamOriginal)

    ssc.start()
    ssc.awaitTermination()


  }

}
