package com.hr.utils
import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.Row
import com.hr.utils.DataSourceUtil._
import org.apache.spark
/**
  * HF
  * 2020-06-22 23:55
  */
object MysqlSink_all {
  Class.forName(real_jdbcDriver) //注册驱动
  //  real_jdbcUrl
  //  real_jdbcUser
  //  real_jdbcPassword
  //  real_jdbcDriver
  def getConnection() = {
    DriverManager.getConnection(real_jdbcUrl, real_jdbcUser, real_jdbcPassword) //获取连接对象
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

  def replaceIntoMysql(Partition: Iterator[Row],replaceIntoMysqlTableName:String,mysqlFieldCount:Int) = {
    var connect: Connection = null
    var pstmt: PreparedStatement = null
    var record: Row = null
    var records = Partition.toList



    //val sql2 = s"insert into ${replaceIntoMysqlTableName} values(?,?,?,?,?)"
    var sql = s"replace into ${replaceIntoMysqlTableName} values("
    for (j <- 1 until mysqlFieldCount) {
      sql = sql + "?,"
    }
    sql = sql + "?)"
    //println(sql)

    try {
      connect = getConnection()   //获取连接对象
      connect.setAutoCommit(false)
      pstmt = connect.prepareStatement(sql) //获取预编译对象

      for (j <- 0 until records.length) {
        record = records(j)

        for ( k <- 1 to mysqlFieldCount ) {
          pstmt.setString(k, record.get(k-1).toString)
        }
        pstmt.addBatch()  //一个数据一次
      }
      try {
        pstmt.executeBatch() //所有数据提交
      }catch{
       case e: Exception => e.printStackTrace();e.getMessage
      }finally{

      }

      connect.commit()
    } catch {
      case e: Exception => e.printStackTrace();e.getMessage
    } finally {
      release(connect, pstmt)

    }


  } //方法结束



} //类结束
