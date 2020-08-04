package com.hr.offline.test

/**
  * HF
  * 2020-06-07 19:47
  */
object test3_time {
  def main(args: Array[String]): Unit = {
    var str = "202007120710"
    var s = s"${str.substring(0,4)}-${str.substring(4,6)}-${str.substring(6,8)} ${str.substring(8,10)}:${str.substring(10,12)}"
println(s)

  }
}
