//package com.hr.realtime
//
//import java.text.SimpleDateFormat
//import java.util
//import java.util.Date
//
//import com.alibaba.fastjson.JSON
//import breeze.numerics.log
//import com.hr.{bean, utils}
//import com.hr.utils._
//import org.apache.spark.{SparkConf, SparkContext, rdd}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
//import java.util.Properties
//import java.text.SimpleDateFormat
//
//import com.hr.bean.{EtcGantryEtcBillInfo, EtcGantryVehDisDataInfo}
//import com.hr.utils.definitionFunction._
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark
//import org.apache.spark.sql.Row
//
//import scala.util.control.Breaks
//
///**
//  * HF
//  * 2020-06-05 11:13
//  */
//object realTime_etc_join_viu_tellRow {
//  def main(args: Array[String]): Unit = {
//
//    var etc_windowDuration = 30
//    var frontAndBackLength = 200
//    var needPrintdetails = "true"    //打印详细信息
//    // 1. 从kafka实时消费数据
//    val conf = new SparkConf()
//      .setAppName("realTime_etc_join_viu_tellRow")
//      .set("spark.streaming.kafka.maxRatePerPartition", "10")   //控制每个分区每秒钟拉取的数量
//      .set("spark.streaming.stopGracefullyOnShutdown", "true")  //开启背压机制
//      //.set("spark.driver.allowMultipleContexts", "true")
//      .setMaster("local[*]")
//
//    //conf.set("spark.driver.allowMultipleContexts","true");
//    println(s"-----${conf.toString}")
//    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
//    val sc: SparkContext = ssc.sparkContext
//    val groupId = "hf8" //消费组,测试使用
//
//
//
//    //从kafka获取etc数据流
//    val etcDStreamOriginal = MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_ETC, groupId)
//    val etcDStream = etcDStreamOriginal.map(item => item.value())
//
//    //从kfka获取viu数据流
//    val viuDStreamOriginal = MySeniorKafkaUtil.getKafkaStream(ssc, DataBaseConstant.TOPIC_VIU, groupId)
//    val viuDStream =  viuDStreamOriginal.map(item => item.value())
//    println(s"--------原始数据--------}")
//    etcDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"AAA----etc的数据----${x.toString}")))
//    viuDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"BBB----viu的数据----${x.toString}")))
//
//
//
//    //广播kafka的生产者
//    //MySeniorKafkaUtil.broadCastKafkaProducer(ssc)
//    //Minutes(60),Seconds(600),//返回的元组第一个是key不用管,第二个是value
//    var kafkaProducer: Broadcast[KafkaSink[String, String]] = {
//      val kafkaProducerConfig = {
//        val p = new Properties()
//        p.setProperty("bootstrap.servers", ConfigurationManager.getProperty("bootstrap.servers"))
//        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
//        p
//      }
//      ssc.sparkContext.broadcast(utils.KafkaSink[String, String](kafkaProducerConfig))
//
//    }
//
////对两个数据流能做开窗处理,保证滑动距离 + 2分钟 <=  etc的窗口大小 ,必须是批次的整数倍
//    val etcWindowsGantryidDStream= etcDStream.map { // 对数据格式做调整
//      case value => {
//        val etcgantryEtcBillInfo = JSON.parseObject(value, classOf[EtcGantryEtcBillInfo])
//        etcgantryEtcBillInfo.transTime = etcgantryEtcBillInfo.transTime.replace("T", " ")
//        var etcrowTmp = Row(
//          etcgantryEtcBillInfo.tradeid,
//          etcgantryEtcBillInfo.vehicleplate,
//          etcgantryEtcBillInfo.transtime,
//          tranTimeToLong(etcgantryEtcBillInfo.transtime),
//          etcgantryEtcBillInfo.gantryid,
//          getCurrentDate()
//        )
//        println(s"${etcrowTmp.getAs[String](4)}---etcrowTmp是:${etcrowTmp}")
//        etcrowTmp
//      }
//    }
//      .window(Seconds(10), Seconds(10)).map(t => (t.getAs[String](4) , t)).groupByKey()
//
//    //Minutes(10), Minutes(8)      Minutes(12), Minutes(8)
//    val viuWindowsGantryidDStream = viuDStream.map {
//      case value => {
//        val etcGantryVehDisDataInfo = JSON.parseObject(value, classOf[EtcGantryVehDisDataInfo])
//        etcGantryVehDisDataInfo.picTime = etcGantryVehDisDataInfo.picTime.replace("T", " ")
//        var viurowTmp = Row(
//          etcGantryVehDisDataInfo.picId ,
//          etcGantryVehDisDataInfo.vehiclePlate,
//          etcGantryVehDisDataInfo.picTime,
//          tranTimeToLong(etcGantryVehDisDataInfo.picTime),
//          etcGantryVehDisDataInfo.gantryId
//          )
//        println(s"${viurowTmp.getAs[String](4)}---viurowTmp:${viurowTmp}")
//        viurowTmp
//      }
//    }
//       .window(Seconds(10), Seconds(10)).map(t => {(t.getAs[String](4), t)}).groupByKey()
//
//    //原始数据写入kafaka有写特殊暂时不管
////    etcDStreamOriginal.map(x=>(1,2)).foreachRDD(rdd=>{
////      rdd.foreach(record=>{
////        println("----开始西写入kafka---")//record.value().toString()
////        kafkaProducer.value.send(DataBaseConstant.TOPIC_RESULT,record.toString()) //写入resultData主题
////      })
////    })
//
//
//    println("------中间------")
//
//    etcWindowsGantryidDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"@@@@@${x}")))
//    viuWindowsGantryidDStream.foreachRDD(rdd=>rdd.foreach(x=>println(s"#####${x}")))
//    //广播变量
//    println("开始广播")
//    val broadcastVar = sc.broadcast(frontAndBackLength)
//    println("结束广播")
//    var etc_join_viu: DStream[(String, String, String, String, String, String, Int, Int)] = etcWindowsGantryidDStream.cogroup(viuWindowsGantryidDStream).flatMap(
//      t => {
//        println("_________开始拟合_________")
//        var etctuIterTmp: Iterable[Row] = null //可迭代的
//        var viuIterTmp: Iterable[Row] = null
//
//        var key = t._1    //etctu时间窗口开始和门架号
//
//        if (t._2._1.iterator.hasNext) etctuIterTmp = t._2._1.iterator.next() //是etc
//        if (t._2._2.iterator.hasNext) viuIterTmp = t._2._2.iterator.next() //是viu
//val a = t._2._1
//
//        if (etctuIterTmp == null) etctuIterTmp = List(): List[Row]
//        if (viuIterTmp == null) viuIterTmp = List(): List[Row]
//
//        //var frontAndBackLength_task = broadcastVar.value //前后相差几分钟
//        var  frontAndBackLength_task = frontAndBackLength
//        var etctuIter = etctuIterTmp.iterator
//        var viuIter = viuIterTmp.iterator
//        var greaterThanMinTime: Boolean = false //大于最小值
//        var lessThanMaxTime: Boolean = true //小于最大值
//        var sign = 0 //标志位,0表是没有拟合成功,1表是拟合成功
//        var (viuCurrentTime, etcCurrentMinTime, etcCurrentMaxTime): (Long, Long, Long) = (0, 0, 0)
//        var matching_degree = 0
//        var matching_degreeInitial = 66 //初始拟合度
//
//        //-----最后结果---roekey_etcid,viuid,运行时间,etc照片,viu照片,etcid,拟合状态,拟合度(总共8个),java.sql.Timestamp
//        var results = List[(String, String, String, String, String, String, Int, Int)]()
//
//        //etc相关变量定义
//        var (etc_tradeid, etc_vehicleplate, etc_transtime, etc_transtime_unix, etc_gantryId, etc_run_relation_time) = ("", "", "", Long, "", "")
//        var rowkey_etc_tradeid = "" //etc_tradeid得到rowkey
//
//        //viu相关变量定义
//        var (viu_picid, viu_vehicleplate, viu_picTime, viu_picTime_unix, viu_gantryId) = ("", "", "", Long, "")
//        var viu_vehicleplate_tmp = ""
//        val etcMembers  = etctuIter.toList
//
//        val viuMembers = viuIter.toList.map(x => {
//          (x.getAs[String](0), x.getAs[String](1), x.getAs[String](2), x.getLong(3), x.getAs[String](4))}) //getString方法不行,why?get(5)可以
//          .sortWith((x, y) => {(x._4) < (y._4) }) //升序排列,3和4都可以,从1开始
//
//
//        var etcLength = etcMembers.length
//        var viuLength = viuMembers.length
//        var etcMembersIsEmpty =  etcLength == 0
//        var viuMembersIsEmpty = viuLength == 0
//
//        if(needPrintdetails == "true"){
//          println(s"key:${key}--viu的长度是:${viuMembers.length}(${viuMembersIsEmpty})----etc的长度:${etcLength}(${etcMembersIsEmpty})" )
//        }  //打印两个集合的信息
//
//        //Breaks.break()
//        //Breaks.breakable(
//
//        //下面开始迭代
//        if (!etcMembersIsEmpty){   //etc不是空才有意义
//          //同时遍历etctu和viu的集合
//          for(j <- 0 until etcMembers.length){
//            var etctuIterRow = etcMembers(j)
//            sign = 0 //拟合状态
//            etc_vehicleplate = etctuIterRow.getString(1)   //etc图片
//            etcCurrentMinTime = etctuIterRow.getLong(3) - frontAndBackLength_task  //秒
//            etcCurrentMaxTime = etctuIterRow.getLong(3) + frontAndBackLength_task
//            if(viuMembersIsEmpty){
//              rowkey_etc_tradeid = HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100) //Row_etcID
//              viu_picid = "" //viu_picid
//              etc_run_relation_time = etctuIterRow.getString(5) //运行时间
//              etc_vehicleplate = etctuIterRow.getString(1) //etc照片
//              viu_vehicleplate = "" //viu照片
//              etc_tradeid = etctuIterRow.getString(0) //etcid
//              sign = 0 //拟合状态
//              matching_degree = matching_degreeInitial //拟合度
//
//              var etctu_join_viu_member_real = (rowkey_etc_tradeid, viu_picid, etc_run_relation_time, etc_vehicleplate, viu_vehicleplate, etc_tradeid, sign, matching_degree)
//              results :+= etctu_join_viu_member_real
//            }
//
//            if(!viuMembersIsEmpty){
//              Breaks.breakable(    //Breaks.break()   !lessThanMaxTime ||
//
//                for (i <- 0 until viuMembers.length) {
//                  viuCurrentTime = viuMembers(i)._4
//                  greaterThanMinTime = viuCurrentTime >= etcCurrentMinTime
//                  lessThanMaxTime = viuCurrentTime <= etcCurrentMaxTime
//                  if (greaterThanMinTime && lessThanMaxTime  ) { //调用自定义函数判断或者==
//                    viu_vehicleplate_tmp = viuMembers(i)._2   //viu图片
//                    matching_degree = 99 //99或者minDistance2(vehicleplate1,vehicleplate2,true)
//                    if (etc_vehicleplate == viu_vehicleplate_tmp) {
//
//                      //rowkey_etc_tradeid = HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100) //Row_etcID ,这里不做处理
//                      viu_picid = viu_picid + "/" + viuMembers(i)._1 //viu_picid
//                      etc_run_relation_time = etctuIterRow.getString(5) //运行时间
//                      //etc_vehicleplate = etctuIterRow.getString(1) //etc照片
//                      viu_vehicleplate = viuMembers(i)._2 //viu照片
//                      etc_tradeid = etctuIterRow.getString(0) //etcid
//                      sign = 1 //拟合状态
//                      matching_degree = matching_degree //拟合度
//                    }
//                  }
//                  if ( !lessThanMaxTime || i == viuMembers.length - 1) {
//                    if (sign == 1) {
//                      rowkey_etc_tradeid = utils.HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100)
//                      var etctu_join_viu_member_real = (rowkey_etc_tradeid, viu_picid, etc_run_relation_time, etc_vehicleplate, viu_vehicleplate, etc_tradeid, sign, matching_degree)
//
//                      println("---元组是:" + etctu_join_viu_member_real)
//                      results :+= etctu_join_viu_member_real
//                      println(s"key:${key}--(${etc_vehicleplate}=${viu_vehicleplate})BBBB(${viuMembers(i)._2}-----")
//                    }
//                    else if (sign == 0) {
//                      rowkey_etc_tradeid = utils.HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100) //Row_etcID
//                      viu_picid = "" //viu_picid
//                      etc_run_relation_time = etctuIterRow.getString(5)//运行时间
//                      etc_vehicleplate = etctuIterRow.getString(1) //etc照片
//                      viu_vehicleplate = "" //viu照片
//                      etc_tradeid = etctuIterRow.getString(0) //etcid
//                      sign = 0 //拟合状态
//                      matching_degree = matching_degreeInitial //拟合度
//
//                      var etctu_join_viu_member_real = (rowkey_etc_tradeid, viu_picid, etc_run_relation_time, etc_vehicleplate, viu_vehicleplate, etc_tradeid, sign, matching_degree)
//                      results :+= etctu_join_viu_member_real
//                    }
//                    else {
//                      var etctu_join_viu_member_real = ("3", "3", "3", "3", "3", "3", 3, 3)
//                      results :+= etctu_join_viu_member_real
//                    }
//                    Breaks.break()
//                  }
//                }
//              )
//            }
//          }
//        }
//        results.iterator
//      })
//
//
//
//    //写入kafka的例子
////    etc_join_viu.foreachRDD(rdd=>{
////      rdd.foreach(record=>{
////        kafkaProducer.value.send("lj03",record)
////      })
////    etc_join_viu.count()
//    println(s"----etc_join_viu结果-----")
//    etc_join_viu.foreachRDD(rdd=>rdd.foreach(x=>println(s"BBB----etc_join_viu的数据----${x.toString}")))
//    etc_join_viu.foreachRDD(rdd=>{
//      rdd.foreach(record=>{
//        println(s"----开始写入kafka:${record}--${record.toString()}")
//        kafkaProducer.value.send(DataBaseConstant.TOPIC_RESULT,record.toString()) //写入resultData主题
//      })
//    })
//
//
//
//
//    println(s"----开始提交偏移量-----")
//    //手动提交偏移量
//    MySeniorKafkaUtil.submitKfkaOffset(groupId,etcDStreamOriginal)
//    MySeniorKafkaUtil.submitKfkaOffset(groupId,viuDStreamOriginal)
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//  }
//
//}
