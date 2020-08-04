package com.hr.utils

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.catalyst.expressions.CurrentDate

import scala.util.matching.Regex

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
    if (50<= CurrentMinute_tmp && CurrentMinute_tmp <= 60){  MinutesPeriod_min= "50";MinutesPeriod_max= "60"}
    println(CurrentMinute_tmp,MinutesPeriod_min,MinutesPeriod_max)
    var MinutesPeriod_real = s"${CurrentDayAndHour}:${MinutesPeriod_min}:00,${CurrentDayAndHour}:${MinutesPeriod_max}:00"
    MinutesPeriod_real
  }

  def getMinutesPeriod2():String={
    var MinutesPeriod_min = ""
    var MinutesPeriod_max = ""
    var CurrentMinute_tmp=getCurrentDate().substring(14,16).toInt
    var CurrentDayAndHour =getCurrentDate().substring(0,13)
    if (0 <= CurrentMinute_tmp && CurrentMinute_tmp < 10){   MinutesPeriod_min= "00";MinutesPeriod_max= "10"}
    if (10<= CurrentMinute_tmp && CurrentMinute_tmp < 20){   MinutesPeriod_min= "10";MinutesPeriod_max= "20"}
    if (20<= CurrentMinute_tmp && CurrentMinute_tmp < 30){   MinutesPeriod_min= "20";MinutesPeriod_max= "30"}
    if (30<= CurrentMinute_tmp && CurrentMinute_tmp < 40){   MinutesPeriod_min= "30";MinutesPeriod_max= "40"}
    if (40<= CurrentMinute_tmp && CurrentMinute_tmp < 50){   MinutesPeriod_min= "40";MinutesPeriod_max= "50"}
    if (50<= CurrentMinute_tmp && CurrentMinute_tmp <= 60){  MinutesPeriod_min= "50";MinutesPeriod_max= "60"}
    println(CurrentMinute_tmp,MinutesPeriod_min,MinutesPeriod_max)
    var MinutesPeriod_real = s"${CurrentDayAndHour}:${MinutesPeriod_min}:00,${CurrentDayAndHour}:${MinutesPeriod_max}:00"
    MinutesPeriod_real
  }

  def getRSUStatusMachedCount(patternStr:String,patternedStr:String):Int = {
    val pattern = new Regex(patternStr)
    val MachedCount = (pattern findAllIn   patternedStr).length
    MachedCount
  }

  def getRSUStatusMachedCorrectCount(patternStr:String,patternedStr:String,correctState:String):Int = {
    val pattern = new Regex(patternStr)
    val matchedIterator: Regex.MatchIterator = (pattern findAllIn   patternedStr)
    var nums: List[Int] = List()
    while (matchedIterator.hasNext){
      var realState = matchedIterator.next()
       if (realState.substring(13,15) == correctState){
         nums :+= 1
       }
    }
    nums.length
  }

  def getALLMachedString(patternStr:String,patternedStr:String):List[String] = {
    val pattern = new Regex(patternStr)
    val matchedIterator: Regex.MatchIterator = (pattern findAllIn   patternedStr)
    var members: List[String] = List()
    while (matchedIterator.hasNext){
      var realState = matchedIterator.next()
        members :+= realState

    }
    members
  }

  def judgeNotInSpecialtype(str:String) :Boolean={
    if (str == null || str == "")  return true
    import scala.collection.mutable.Set
    var SpecialtypeSet: Set[Int] = Set(155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184)

    var strSet: Set[Int]  = Set()
    //println(s"切割的数据是:${str}")
    str.split('|').foreach(x=>{
      //println(s"切割的数据是:${x}")
      strSet = strSet + x.toInt
    })
    var result = true
    if (SpecialtypeSet.intersect(strSet).size > 0) {
      result = false
    } else {
      result = true
    }
    result
  }
  def main(args: Array[String]): Unit = {
    println(getCurrentDate())
    println(getCurrentDate().substring(14,16))
    println(getMinutesPeriod())
    println(judgeNotInSpecialtype(null))
  }

}
