package com.hr.offline.tableRelation

import com.hr.utils
import com.hr.utils.{HBaseGeneral, HbaseUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession, functions}

/**
  * HF
  * 2020-05-28 14:42
  */


object etctu_join_viu_all_tellRow2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)  //前后相差的秒数必须是etc窗口大小的倍数
    var ( etc_windowDuration,frontAndBackLength, wrongCount, year, month, day, hour, day_or_hour,needPrintdetails) = (args(0).toInt, args(1).toInt, args(2), args(3), args(4), args(5), args(6), args(7), args(8))
    println(s"""---信息  窗口:${etc_windowDuration}-相差${frontAndBackLength}秒-${wrongCount},消费${year}年${month}月${day}号${hour}点的数据-粒度:${day_or_hour}-是否打印详细信息:${needPrintdetails}  """) //(etctuCount, viuCount, wrongCount,year,month,day)
    println("--------版本-19:14---------")
    val spark = SparkSession.builder()
      .master("yarn-cluster") //local[*],yarn-cluster
      .appName(s"粒度:${day_or_hour} - 窗口:${etc_windowDuration}秒 - 前后${frontAndBackLength}秒 - ${month}月${day}日 - ${wrongCount}")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    import org.apache.spark.sql.functions._
    import spark.implicits._
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    spark.conf.set("spark.storage.memoryFraction", "0.4")
    spark.conf.set("hbase.master", "ip:port")

    //spark自定义函数,用来判断 两个牌示字符串的距离
//    val minDistance2: (String, String, Boolean) => Int = (str: String, target: String, isDefaultlicense: Boolean) => {
//      var (n, m, k) = (0, 0, 0)
//      n = if (n == null || (isDefaultlicense && str == "默A00000_9")) 0 else str.length
//      m = if (m == null) 0 else target.length
//      var distance: Array[Array[Int]] = Array.ofDim[Int](n + 1, m + 1)
//      0.until(n).foreach(i => distance(i)(0) = i)
//      0.until(m).foreach(j => distance(0)(j) = j)
//      1.to(n).foreach(i => 1.to(m).foreach(j => (
//        distance(i)(j) = if (str.charAt(i - 1) != target.charAt(j - 1)) {
//          Math.min(distance(i - 1)(j) + 1, Math.min(distance(i)(j - 1) + 1, distance(i - 1)(j - 1) + 1))
//        } else {
//          Math.min(distance(i - 1)(j) + 1, Math.min(distance(i)(j - 1) + 1, distance(i - 1)(j - 1)))
//        }
//        )))
//      k = if (m * n == 0) 9 else distance(n)(m);
//      distance = null;
//      k
//    }

//    spark.udf.register("tell_etctu_viu_join", (str: String, target: String) => minDistance2(str: String, target: String, true: Boolean))
    val rowGetNotNullString: (Row, Int) => String = (row: Row, index: Int) => {
      if (row.isNullAt(index)) "" else row.getAs[String](index)
    }
    spark.udf.register("rowGetNotNullString", (row: Row, index: Int) => rowGetNotNullString(row: Row, index: Int))
    val rowGetNotNullLong: (Row, Int) => Long = (row: Row, index: Int) => {
      if (row.isNullAt(index)) 0 else row.getAs[Long](index)
    }
    spark.udf.register("rowGetNotNullLong", (row: Row, index: Int) => rowGetNotNullLong(row: Row, index: Int))


    spark.sql("use  ODS")
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    var etcSql = ""
    var etcSql_hour =
      s"""
         |select tradeid  as etc_tradeid,
         |vehicleplate    as etc_vehicleplate,
         |regexp_replace(transtime,'T',' ') as  etc_transtime,
         |unix_timestamp(regexp_replace(transtime,'T',' ')) etc_transtime_unix,
         |gantryId as etc_gantryId,
         |current_timestamp etc_run_relation_time
         |from ODS.ETC_gantryEtcBill
         |where year='${year}' and month='${month}' and day='${day}' and hour='${hour}'
       """.stripMargin
    var etcSql_day =
      s"""
         |select tradeid  as etc_tradeid,
         |vehicleplate    as etc_vehicleplate,
         |regexp_replace(transtime,'T',' ') as  etc_transtime,
         |unix_timestamp(regexp_replace(transtime,'T',' ')) as etc_transtime_unix,
         |gantryId as etc_gantryId,
         |current_timestamp as etc_run_relation_time
         |from ODS.ETC_gantryEtcBill
         |where year='${year}' and month='${month}' and day='${day}'
       """.stripMargin

    etcSql = if (day_or_hour == "day") etcSql_day else etcSql_hour
    println("--------------从etc取数据的sql语句--------");
    println(etcSql)
    var etctu_tmp = spark.sql(etcSql).repartition(50).withColumn("etc_windows", functions.window($"etc_transtime", s"${etc_windowDuration} second", s"${etc_windowDuration} second")).withColumn("etc_windows_start", functions.unix_timestamp(col("etc_windows.start")))

    if(needPrintdetails == "true") { //打印etctu_tmp(表)的长度
      var etctu_tmp_length = etctu_tmp.count()
      println("--------etctu_tmp 的长度是:" + etctu_tmp_length)
      println("--------etctu_tmp 的去重后的长度是:" + etctu_tmp.distinct().count())
    }

    var etctu = etctu_tmp.rdd.groupBy(x => (x.get(4) + "-" + x.get(7))) //从0开始


    var viuSql = ""
    var viuSql_hour =
      s"""
         |select picid as viu_picid,
         |vehicleplate as viu_vehicleplate,
         |regexp_replace(picTime,'T'," ")  viu_picTime,
         |unix_timestamp(regexp_replace(picTime,'T'," ")) viu_picTime_unix,
         |gantryId as viu_gantryId
         |from ODS.ETC_gantryVehDisData
         |where year='${year}' and month='${month}' and day='${day}' and hour='${hour}'
       """.stripMargin
    var viuSql_day =
      s"""
         |select picid as viu_picid,
         |vehicleplate as viu_vehicleplate,
         |regexp_replace(picTime,'T'," ")  viu_picTime,
         |unix_timestamp(regexp_replace(picTime,'T'," ")) viu_picTime_unix,
         |gantryId as viu_gantryId
         |from ODS.ETC_gantryVehDisData
         |where year='${year}' and month='${month}' and day='${day}'
       """.stripMargin
    viuSql = if (day_or_hour == "day") viuSql_day else viuSql_hour
    println("--------------从viu取数据的sql语句--------");
    println(viuSql)
    var viu_windowDuration = 5 + 2*frontAndBackLength
    var viu_slideDuration = etc_windowDuration

    println(s"etc_windowDuration:${etc_windowDuration},etc_slideDuration${etc_windowDuration}")
    //开始时间的绝对值要小于滑动时间
    println(s"viu_windowDuration:${viu_windowDuration},viu_slideDuration:${viu_slideDuration}")

    var viu = spark.sql(viuSql).repartition(53).withColumn("viu_windows", functions.window($"viu_picTime", s"${viu_windowDuration} second", s"${viu_slideDuration} second")).withColumn("viu_windows_start_add_2mintu", functions.unix_timestamp(col("viu_windows.start")) + frontAndBackLength).rdd.groupBy(x => (x.get(4) + "-" + x.get(6)))


    import scala.util.control.Breaks
    //广播变量
    val broadcastVar = sc.broadcast(frontAndBackLength)
    var g_etctu_g_viu_relatition_result = etctu.cogroup(viu).repartition(300).flatMap(
      t => {
        var viuIterTmp: Iterable[Row] = null
        var etctuIterTmp: Iterable[Row] = null //可迭代的
        var key = t._1    //etctu时间窗口开始和门架号
        if (t._2._1.iterator.hasNext) etctuIterTmp = t._2._1.iterator.next() //是etc
        if (t._2._2.iterator.hasNext) viuIterTmp = t._2._2.iterator.next() //是viu
        if (etctuIterTmp == null) etctuIterTmp = List(): List[Row]
        if (viuIterTmp == null) viuIterTmp = List(): List[Row]
        var frontAndBackLength_task = broadcastVar.value //前后相差几分钟
        var etctuIter = etctuIterTmp.iterator
        var viuIter = viuIterTmp.iterator
        var greaterThanMinTime: Boolean = false //大于最小值
        var lessThanMaxTime: Boolean = true //小于最大值
        var sign = 0 //标志位,0表是没有拟合成功,1表是拟合成功
        var (viuCurrentTime, etcCurrentMinTime, etcCurrentMaxTime): (Long, Long, Long) = (0, 0, 0)
        var matching_degree = 0
        var matching_degreeInitial = 66 //初始拟合度

        //-----最后结果---roekey_etcid,viuid,运行时间,etc照片,viu照片,etcid,拟合状态,拟合度(总共8个),java.sql.Timestamp
        var results = List[(String, String, String, String, String, String, Int, Int)]()

        //etc相关变量定义
        var (etc_tradeid, etc_vehicleplate, etc_transtime, etc_transtime_unix, etc_gantryId, etc_run_relation_time) = ("", "", "", Long, "", "")
        var rowkey_etc_tradeid = "" //etc_tradeid得到rowkey

        //viu相关变量定义
        var (viu_picid, viu_vehicleplate, viu_picTime, viu_picTime_unix, viu_gantryId) = ("", "", "", Long, "")
        var viu_vehicleplate_tmp = ""
        val etcMembers  = etctuIter.toList
        val viuMembers = viuIter.toList.map(x => {
          (x.getAs[String]("viu_picid"), x.getAs[String]("viu_vehicleplate"), x.getAs[String]("viu_picTime"), x.getAs[Long]("viu_picTime_unix"), x.getAs[String]("viu_gantryId"), x.getAs[String]("viu_gantryId"), x.getAs[Long]("viu_windows_start_add_2mintu"))}) //getString方法不行,why?get(5)可以
          .sortWith((x, y) => {(x._4) < (y._4) }) //升序排列,3和4都可以,从1开始


        var etcLength = etcMembers.length
        var viuLength = viuMembers.length
        var etcMembersIsEmpty =  etcLength == 0
        var viuMembersIsEmpty = viuLength == 0

        if(needPrintdetails == "true"){
          println(s"key:${key}--viu的长度是:${viuMembers.length}(${viuMembersIsEmpty})----etc的长度:${etcLength}(${etcMembersIsEmpty})" )
        }  //打印两个集合的信息

        //Breaks.break()
        //Breaks.breakable(

        //下面开始迭代
        if (!etcMembersIsEmpty){   //etc不是空才有意义
          //同时遍历etctu和viu的集合
          for(j <- 0 until etcMembers.length){
            var etctuIterRow = etcMembers(j)
            sign = 0 //拟合状态
            etc_vehicleplate = etctuIterRow.getString(1)   //etc图片
            etcCurrentMinTime = etctuIterRow.getLong(3) - frontAndBackLength_task  //秒
            etcCurrentMaxTime = etctuIterRow.getLong(3) + frontAndBackLength_task
            if(viuMembersIsEmpty){
              rowkey_etc_tradeid = HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100) //Row_etcID
              viu_picid = "" //viu_picid
              etc_run_relation_time = etctuIterRow.getTimestamp(5).toString //运行时间
              etc_vehicleplate = etctuIterRow.getString(1) //etc照片
              viu_vehicleplate = "" //viu照片
              etc_tradeid = etctuIterRow.getString(0) //etcid
              sign = 0 //拟合状态
              matching_degree = matching_degreeInitial //拟合度

              var etctu_join_viu_member_real = (rowkey_etc_tradeid, viu_picid, etc_run_relation_time, etc_vehicleplate, viu_vehicleplate, etc_tradeid, sign, matching_degree)
              results :+= etctu_join_viu_member_real
            }

            if(!viuMembersIsEmpty){
              Breaks.breakable(    //Breaks.break()   !lessThanMaxTime ||

                for (i <- 0 until viuMembers.length) {
                  viuCurrentTime = viuMembers(i)._4
                  greaterThanMinTime = viuCurrentTime >= etcCurrentMinTime
                  lessThanMaxTime = viuCurrentTime <= etcCurrentMaxTime
                  if (greaterThanMinTime && lessThanMaxTime  ) { //调用自定义函数判断或者==
                    viu_vehicleplate_tmp = viuMembers(i)._2   //viu图片
                    matching_degree = 99 //99或者minDistance2(vehicleplate1,vehicleplate2,true)
                    if (etc_vehicleplate == viu_vehicleplate_tmp) {

                      //rowkey_etc_tradeid = HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100) //Row_etcID ,这里不做处理
                      viu_picid = viu_picid + "/" + viuMembers(i)._1 //viu_picid
                      etc_run_relation_time = etctuIterRow.getTimestamp(5).toString //运行时间
                      //etc_vehicleplate = etctuIterRow.getString(1) //etc照片
                      viu_vehicleplate = viuMembers(i)._2 //viu照片
                      etc_tradeid = etctuIterRow.getString(0) //etcid
                      sign = 1 //拟合状态
                      matching_degree = matching_degree //拟合度
                    }
                  }
                  if ( !lessThanMaxTime || i == viuMembers.length - 1) {
                    if (sign == 1) {
                      rowkey_etc_tradeid = utils.HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100)
                      var etctu_join_viu_member_real = (rowkey_etc_tradeid, viu_picid, etc_run_relation_time, etc_vehicleplate, viu_vehicleplate, etc_tradeid, sign, matching_degree)

                      println("---元组是:" + etctu_join_viu_member_real)
                      results :+= etctu_join_viu_member_real
                      println(s"key:${key}--(${etc_vehicleplate}=${viu_vehicleplate})BBBB(${viuMembers(i)._2}-----")
                    }
                    else if (sign == 0) {
                      rowkey_etc_tradeid = utils.HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100) //Row_etcID
                      viu_picid = "" //viu_picid
                      etc_run_relation_time = etctuIterRow.getTimestamp(5).toString //运行时间
                      etc_vehicleplate = etctuIterRow.getString(1) //etc照片
                      viu_vehicleplate = "" //viu照片
                      etc_tradeid = etctuIterRow.getString(0) //etcid
                      sign = 0 //拟合状态
                      matching_degree = matching_degreeInitial //拟合度

                      var etctu_join_viu_member_real = (rowkey_etc_tradeid, viu_picid, etc_run_relation_time, etc_vehicleplate, viu_vehicleplate, etc_tradeid, sign, matching_degree)
                      results :+= etctu_join_viu_member_real
                    }
                    else {
                      var etctu_join_viu_member_real = ("3", "3", "3", "3", "3", "3", 3, 3)
                      results :+= etctu_join_viu_member_real
                    }
                    Breaks.break()
                  }
                }
              )
            }
          }
        }
        results.iterator
      }).toDF("rowkey_etc_tradeid", "viu_picid", "etc_run_relation_time", "etc_vehicleplate", "viu_vehicleplate", "etc_tradeid", "sign", "matching_degree")
    g_etctu_g_viu_relatition_result.cache()





    if(needPrintdetails == "true") { //打印详细信息
      var g_etctu_g_viu_relatition_result_lenth = g_etctu_g_viu_relatition_result.count()

      println(">>>>>>>>>>>>>>>>g_etctu_g_viu_relatition_result长度是:-------" + g_etctu_g_viu_relatition_result_lenth)
      println(">>>>>>>>>>>>>>>>匹配成功的长度是:-------" + g_etctu_g_viu_relatition_result.where("sign = 1").count())

      println(">>>>>>>>>>>>>>>>匹配失败的长度是:-------" + g_etctu_g_viu_relatition_result.where("sign =0").count())
      var successCount = g_etctu_g_viu_relatition_result.where("sign = 1").distinct().count()
      println(">>>>>>>>>>>>>>>>匹配成功的长度去重后是:-------" + successCount)

      var faileCount = g_etctu_g_viu_relatition_result.where("sign =0").distinct().count()
      println(">>>>>>>>>>>>>>>>匹配失败的长度去重后是:-------" + faileCount)

      println(">>>>>>>>>>>>>>>>匹配失败是3长度是:-------" + g_etctu_g_viu_relatition_result.where("sign = 3").count())
      println(s">>>>>>>>>>>>>>>>全部去重后相加的结果是:${faileCount + successCount}-------")
    }

    var g_etctu_g_viu_relatition_hive_succes = g_etctu_g_viu_relatition_result.where("sign = 1")
    g_etctu_g_viu_relatition_hive_succes.cache()
    g_etctu_g_viu_relatition_hive_succes.repartition(4).createOrReplaceTempView("g_etctu_g_viu_relatition_succes_tmp")

    var g_etctu_g_viu_relatition_hive_faile = g_etctu_g_viu_relatition_result.where("sign = 0").repartition(5).createOrReplaceTempView("g_etctu_g_viu_relatition_faile_tmp")



    var succesSql =
      s"""
         |from g_etctu_g_viu_relatition_succes_tmp
         |INSERT overwrite table spark_test.g_etctu_g_viu_relatition_succes
         |select
         |rowkey_etc_tradeid,
         |viu_picid ,
         |etc_run_relation_time,
         |etc_vehicleplate,
         |viu_vehicleplate
       """.stripMargin
    println("--------------插入hive表g_etctu_g_viu_relatition_hive_tmp_succes的数据的sql语句------");
    println(succesSql)
    spark.sql(succesSql) //插入spark_test库


    var faileSql =
      s"""
         |from g_etctu_g_viu_relatition_faile_tmp
         |INSERT overwrite table spark_test.g_etctu_g_viu_relatition_faile
         |select
         |rowkey_etc_tradeid,
         |viu_picid ,
         |etc_run_relation_time,
         |etc_vehicleplate,
         |viu_vehicleplate
       """.stripMargin
    println("---插入hive表g_etctu_g_viu_relatition_hive_tmp_faile的数据的sql语句------");
    println(faileSql)
    spark.sql(faileSql) //插入spark_test库



    //下面是写入hbase的
    println("----------开始写入hbase表------------\n\n\n");
    var g_etctu_g_viu_relatition_hbase = g_etctu_g_viu_relatition_hive_succes.repartition(120).foreachPartition{list => {//批量写入hbase
    var puts = new java.util.ArrayList[Put]();
      list.foreach( r => {
        //        val tradeId = r(0).toString
        //        val picIds = r(1).toString
        //        var createDate = r(2).toString
        var put = new Put(Bytes.toBytes(HbaseUtil.getRowKey(r(0).toString,"",100,true)))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("picId"),Bytes.toBytes(r(1).toString))
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("createDate"),Bytes.toBytes(r(2).toString))
        puts.add(put)
        if (puts.size() == 10000) {
          HbaseUtil.putToHBaseWithBatch(puts, "freeflow_dev:tb_gantry_etc_pic_relation")
          puts.clear()
          Thread.sleep(500)
        }
      })
      if (puts != null && !puts.isEmpty && puts.size() < 10000) {
        HbaseUtil.putToHBaseWithBatch(puts, "freeflow_dev:tb_gantry_etc_pic_relation")
        puts.clear()
      }
    }}


    println("----------结束写入hbase表------------\n\n\n")
    println("----------现在全部任务运行结束------------")
    //释放资源
    spark.stop()


  }
}
