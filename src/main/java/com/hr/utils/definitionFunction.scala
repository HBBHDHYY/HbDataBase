package com.hr.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.catalyst.expressions.CurrentDate

/**
  * HF
  * 2020-06-07 16:41
  */

object definitionFunction {

  def getCurrentDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    return date
  }

  def getCurrentUnixTimestamp(): Long = {

    val now = new Date()
    val a = now.getTime
    var str = a+""
    str.substring(0,10).toLong
  }

  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }

  def getMinutesPeriod():String={
    var MinutesPeriod_min = ""
    var MinutesPeriod_max = ""
    var CurrentMinute_tmp=getCurrentDate().substring(14,16).toInt
    var CurrentDayAndHour =getCurrentDate().substring(0,13)
    if (0 <= CurrentMinute_tmp && CurrentMinute_tmp < 10){   MinutesPeriod_min= "00";MinutesPeriod_max= "10"}
    if (10<= CurrentMinute_tmp && CurrentMinute_tmp < 20){   MinutesPeriod_min= "10";MinutesPeriod_max= "20"}
    if (20<= CurrentMinute_tmp && CurrentMinute_tmp < 30){   MinutesPeriod_min= "20";MinutesPeriod_max= "30"}
    if (30<= CurrentMinute_tmp && CurrentMinute_tmp < 40){   MinutesPeriod_min= "30";MinutesPeriod_max= "40"}
    if (40<= CurrentMinute_tmp && CurrentMinute_tmp < 50){   MinutesPeriod_min= "40";MinutesPeriod_max= "50"}
    if (50<= CurrentMinute_tmp && CurrentMinute_tmp <= 60){  MinutesPeriod_min= "50";MinutesPeriod_max= "80"}
    println(CurrentMinute_tmp,MinutesPeriod_min,MinutesPeriod_max)
    var MinutesPeriod_real = s"${CurrentDayAndHour}:${MinutesPeriod_min}_${CurrentDayAndHour}:${MinutesPeriod_max}"
    MinutesPeriod_real
  }
  def main(args: Array[String]): Unit = {
    println(getCurrentDate())
    println(getCurrentDate().substring(14,16))
    println(getMinutesPeriod())
  }

}
