package com.hr.utils

import org.apache.spark.sql.ForeachWriter
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext, rdd, sql}
import org.apache.spark.sql.Row
import com.hr.utils.DataSourceUtil._
import org.apache.spark
import com.hr.utils.DataSourceUtil.real_jdbcDriver
import com.hr.utils.MysqlSink_all.{getConnection, release}
import org.apache.spark.sql.{ForeachWriter, Row}
import shapeless.record

/**
  * HF
  * 2020-06-27 4:47
  * replaceIntoMysqlTableName :要插入的mysql的表名
  * mysqlFieldCount :要插入的mysql的表的字段的个数
  */
class MysqlSink_StructuredStreaming(replaceIntoMysqlTableName: String, mysqlFieldCount: Int) extends ForeachWriter[Row] {
  var connect: Connection = null
  var pstmt: PreparedStatement = null
  var result_data :Any = null

  override def open(partitionId: Long, epochId: Long): Boolean = {
println(s"表${replaceIntoMysqlTableName}:开始执行open方法")
    Class.forName(real_jdbcDriver) //注册驱动
    DriverManager.getConnection(real_jdbcUrl, real_jdbcUser, real_jdbcPassword) //获取连接对象
    connect = getConnection() //获取连接对象
    connect.setAutoCommit(false)
    //val sql2 = s"insert into ${replaceIntoMysqlTableName} values(?,?,?,?,?)"
    var sql = s"replace into ${replaceIntoMysqlTableName} values("
    for (j <- 1 until mysqlFieldCount) {
      sql = sql + "?,"
    }
    sql = sql + "?)"
    //println(sql)

    pstmt = connect.prepareStatement(sql) //获取预编译对象
    true
  }

  override def process(value: Row): Unit = {
    //println(s"表${replaceIntoMysqlTableName}:开始执行process方法")
    println(s"--value:${value}")
    try {
      for (k <- 1 to mysqlFieldCount) {
        if (value.get(k - 1) == null) {
          println("---有数据是null----")
          result_data ="null"
          println(s"result_data:${result_data}")
        } else{
          result_data =value.get(k - 1)
        }

        pstmt.setString(k, result_data.toString)
      }
      pstmt.addBatch() //一个数据一次
    } catch {
      case e: Exception => {
        e.printStackTrace()
        e.getMessage
        println(s"表${replaceIntoMysqlTableName}:单次提交出现问题")
      }
    } finally {

    }
  }

  override def close(errorOrNull: Throwable): Unit = {
println(s"表${replaceIntoMysqlTableName}:开始执行close方法")
    try {
      pstmt.executeBatch() //所有数据提交
      connect.commit()
    } catch {
      case e: Exception => {
        println("----下面是批量提交的报错信息-----")
        e.printStackTrace();
        e.getMessage
        println(e)
        println(s"表${replaceIntoMysqlTableName}:批量提交出现问题")
      }
    } finally {
      release(connect, pstmt) //释放资源
    }
  }

  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }


}
