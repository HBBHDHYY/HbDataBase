//package com.hr.offline
//
//import com.hr.utils.{HBaseGeneral, HbaseUtil}
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{Row, SparkSession, functions}
//
///**
//  * HF
//  * 2020-05-28 14:42
//  */
//
//
//object etctu_join_viu_day {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    var (etctuCount, viuCount, wrongCount, year, month, day) = (args(0), args(1), args(2), args(3), args(4), args(5))
//
//    println(s"""信息 etc:${etctuCount}条,viu:${viuCount},${wrongCount},消费${year}年${month}月${day}号点的数据""")
//    //(etctuCount, viuCount, wrongCount,year,month,day)
//    //spark自定义函数,用来判断 两个牌示字符串的距离
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
//
//    System.setProperty("HADOOP_USER_NAME", "hdfs")
//
//    val spark = SparkSession.builder()
//      .master("yarn-cluster") //local[*],yarn-cluster
//      .appName(s"${etctuCount}:${viuCount}:${day}号:${wrongCount}")
//      .enableHiveSupport()
//      .getOrCreate()
//    import org.apache.spark.sql.functions._
//    import spark.implicits._
//
//    spark.conf.set("spark.storage.memoryFraction", "0.4")
//
//    spark.udf.register("tell_etctu_viu_join", (str: String, target: String) => minDistance2(str: String, target: String, true: Boolean))
//
//    spark.sql("use  ODS")
//    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//
//    var s1 =
//      s"""
//         |select tradeid,vehicleplate as vehicleplate1,
//         |regexp_replace(transtime,'T',' ') as  transtime,
//         |unix_timestamp(regexp_replace(transtime,'T',' ')) transtime_unix,
//         |gantryId,
//         |current_timestamp run_relation_time
//         |from ODS.ETC_gantryEtcBill
//         |where year='${year}' and month='${month}' and day='${day}'
//       """.stripMargin
//    println("--------------从etc取数据的sql语句--------");
//    println(s1)
//
//    var etctu = spark.sql(s1).repartition(50).withColumn("windows1", functions.window($"transtime", "5 second", "5 second")).withColumn("windows1_start", functions.unix_timestamp(col("windows1.start"))).rdd.groupBy(x => (x.get(4) + "-" + x.get(7))) //从0开始
//
//
//    var s2 =
//      s"""
//         |select picid,vehicleplate as vehicleplate2,
//         |regexp_replace(picTime,'T'," ")  picTime,
//         |unix_timestamp(regexp_replace(picTime,'T'," ")) picTime_unix,
//         |gantryId
//         |from ODS.ETC_gantryVehDisData
//         |where year='${year}' and month='${month}' and day='${day}'
//       """.stripMargin
//    println("--------------从viu取数据的sql语句--------");
//    println(s2)
//    //开始时间的绝对值要小于滑动时间
//    var viu = spark.sql(s2).repartition(53).withColumn("windows2", functions.window($"picTime", "245 second", "5 second")).withColumn("windows2_start_add_2mintu", functions.unix_timestamp(col("windows2.start")) + 120).rdd.groupBy(x => (x.get(4) + "-" + x.get(6)))
//    import scala.util.control.Breaks
//
//
//    var g_etctu_g_viu_relatition_tmp = etctu.cogroup(viu).repartition(300).flatMap(t => {
//      var a: Iterable[Row] = null
//      var A: Iterable[Row] = null //可迭代的
//      //var k = t._1    //etctu时间窗口开始
//      if (t._2._1.iterator.hasNext) A = t._2._1.iterator.next() //是etc
//      if (t._2._2.iterator.hasNext) a = t._2._2.iterator.next() //是viu
//      if (A == null) A = List(): List[Row]
//      if (a == null) a = List(): List[Row]
//
//      var etctuIter = A.iterator;
//      var viuIter = a.iterator
//      var flag1: Boolean = false //最小边界在外面
//      var flag2: Boolean = true //最大边界在里面
//      var (viuTime, minTime, maxTime): (Long, Long, Long) = (0, 0, 0)
//      var result = List[(String, String, java.sql.Timestamp, String, String)]() //最后插入结果表的字段
//
//      val viuMembers = viuIter.toList.map(x => {
//        (x.getAs[String]("picid"), x.getAs[String]("vehicleplate2"), x.getAs[String]("picTime"), x.getAs[Long]("picTime_unix"), x.getAs[String]("gantryId"), x.getAs[String]("windows2"), x.getAs[Long]("windows2_start_add_2mintu")) //getString方法不行了,why?get(5)可以
//      }).sortWith((x, y) => {
//        (x._4) < (y._4)
//      }) //升序排列,3和4都可以,从1开始
//
//
//      //同时遍历etctu和viu的集合
//      while (etctuIter.hasNext) {
//
//        var etctuIterRow = etctuIter.next()
//
//        var etctu_gantryId = etctuIterRow.getAs[String]("gantryId")
//
//        var (viu_gantryId, etctu_vehicleplate, viu_vehicleplate, row_key,sign) = ("", "", "", "",0)
//        var Matching_degree = 66 //匹配度
//        var minTime = etctuIterRow.getLong(3) - 120
//        var maxTime = etctuIterRow.getLong(3) + 120
//
//
//        Breaks.breakable(
//          for (i <- 0 until viuMembers.length) {
//            viu_gantryId = viuMembers(i)._5
//            viuTime = viuMembers(i)._4
//            //println("viuTime:"  + viuTime)
//            flag1 = viuTime >= minTime
//            flag2 = viuTime <= maxTime
//            if (flag1 && flag2) { //调用自定义函数判断或者==
//              etctu_vehicleplate = etctuIterRow.getString(1)
//              viu_vehicleplate = viuMembers(i)._2
//              Matching_degree = 99 //99或者minDistance2(vehicleplate1,vehicleplate2,true)
//
//
//              if (etctu_vehicleplate == viu_vehicleplate) {
//                sign = 1
//                row_key = HBaseGeneral.getRowKey(etctuIterRow.getString(0), "", 100)
//                var etctu_join_viu_member = (row_key, viuMembers(i)._1, etctuIterRow.getTimestamp(5), etctuIterRow.getString(1), viuMembers(i)._2)
//                result :+= etctu_join_viu_member
//              }
//
//
//            } else if (!flag2) {
//              if( sign == 0){
//                var etctu_join_viu_member = (row_key, viuMembers(i)._1, etctuIterRow.getTimestamp(5), etctuIterRow.getString(1), viuMembers(i)._2)
//                result :+= etctu_join_viu_member
//              }
//
//
//              Breaks.break() //说明当前viuTime大于ecttu的2分钟,没有遍历下去的必要了
//            }
//          }
//        )
//      }
//
//      result.iterator
//    }).toDF("g_etctu_tradeId", "g_viu_picid", "run_relation_time", "vehicleplate1", "vehicleplate2")
//      .repartition(30).foreachPartition { list => {
//      //批量写入hbase
//      var puts = new java.util.ArrayList[Put]();
//      list.foreach(r => {
//        //        val tradeId = r(0).toString
//        //        val picIds = r(1).toString
//        //        var createDate = r(2).toString
//        var put = new Put(Bytes.toBytes(HbaseUtil.getRowKey(r(0).toString, "", 100, true)))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("picId"), Bytes.toBytes(r(1).toString))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("createDate"), Bytes.toBytes(r(2).toString))
//        puts.add(put)
//        if (puts.size() == 10000) {
//          HbaseUtil.putToHBaseWithBatch(puts, "freeflow_dev:tb_gantry_etc_pic_relation")
//          puts.clear()
//          Thread.sleep(500)
//        }
//
//      })
//      if (puts != null && !puts.isEmpty && puts.size() < 10000) {
//        HbaseUtil.putToHBaseWithBatch(puts, "freeflow_dev:tb_gantry_etc_pic_relation")
//        puts.clear()
//      }
//
//    }
//    }
//
//
//
//
//
//
//    //释放资源
//    spark.stop()
//
//
//  }
//}
