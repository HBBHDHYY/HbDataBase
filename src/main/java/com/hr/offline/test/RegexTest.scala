package com.hr.offline.test
import scala.util.matching.Regex
import com.hr.offline.test.test_Constant.GantryInfo
/**
  * HF
  * 2020-06-20 22:25
  */
object RegexTest {
  def main(args: Array[String]) {
    //val pattern = new Regex("\"enTime\":\"\\w{4}-\\w{2}-\\w{2}T\\w{2}:\\w{2}:\\w{2}\"")


    val pattern = new Regex("\"RSUStatus\":\"\\w{2}\"")
    val str = GantryInfo

//val a = (pattern findFirstIn  str).mkString("")
//    println(a)
//    println(a.substring(10,20) +" "+ a.substring(21,29))
//    println("------------")
//    println((pattern findFirstIn  str).mkString("").substring(10,20) +" "+ (pattern findFirstIn  str).mkString("").substring(21,29))
//    println("------------")
//    println(a.replace("T"," "))


    val a = (pattern findAllIn   str)

    while (a.hasNext){
      var b = a.next()
      println("--------")
      println(b)
      println(b.substring(13,15))
      println(b.substring(13,15) == "21")
    }





    println("+++++++++++++++")

import com.hr.utils.definitionFunction._
    var b = getRSUStatusMachedCorrectCount("\"RSUStatus\":\"\\w*\"",GantryInfo,"21")
    println(b)

    println("OOOOOOOOOOOOOOO")
    var c = getALLMachedString("statusCode\":\"\\w*",GantryInfo)
    println(c)
    println(c(0).substring(13,c(0).length))

  }
}
