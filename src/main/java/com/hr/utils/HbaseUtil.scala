package com.hr.utils

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes


object HbaseUtil {

  /**
    * 按批次put进入hbase
    * @param puts
    * @param tableName
    */
  def putMsg(puts : java.util.ArrayList[Put],tableName : String)={
    val hbaseConfig : Configuration = HBaseConfiguration.create
    val connection : Connection = ConnectionFactory.createConnection(hbaseConfig)
    val table : HTable = connection.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
    table.put(puts)
    table.close()
    connection.close()
  }

  /**
    * 获取hbase的rowKey
    * @param countKey
    * @param suffix
    * @param regionNum
    * @param isNewTable
    */
  def getRowKey(countKey:String,suffix:String,regionNum:Int,isNewTable:Boolean): String ={

    if (regionNum > 1) {
      val regionCode = getRegionCode(countKey, regionNum, isNewTable)
      regionCode + countKey + suffix
    } else {
      countKey + suffix
    }

  }

  /**
    * 计算分区前缀
    * @param countKey
    * @param regionNum
    * @param isNewTable
    * @return
    */
  def getRegionCode(countKey:String,regionNum:Int,isNewTable:Boolean): Any ={
    if (countKey != null && !("" == countKey)) {
      if (isNewTable) {
        var result = "";
        var regionCode = Math.abs(countKey.hashCode) % regionNum
        val codeLength = (regionCode + "").length
        var totalLength = 0
        if (regionNum <= 10) totalLength = 2
        else if (100 == regionNum) totalLength = 2
        else if (1000 == regionNum) totalLength = 3
        else totalLength = (regionNum + "").length
        for (i <- 0 until totalLength - codeLength) {
          if (result == "") "0" + regionCode
          else "0" + result
        }
        if (result == "") regionCode + ""
      } else {
        var regionCode = Math.abs(countKey.hashCode) % regionNum
        if (regionCode < 10) "0" + regionCode
        else regionCode + ""
      }
    } else null
  }

  //  def main(args: Array[String]): Unit = {
  //    println(getRowKey("12323456789","sf",1000,true))
  //  }


   def putToHBaseWithBatch(puts : java.util.ArrayList[Put],tableName : String)={
    var table: HTable = null;
    var connection: Connection = null;
    try {
      val hbaseConfig: Configuration = HBaseConfiguration.create
      connection = ConnectionFactory.createConnection(hbaseConfig)
      table = connection.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
      table.put(puts)
    }finally {
      table.close()
      connection.close()
    }
  }

}
