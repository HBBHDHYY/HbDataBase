package com.hr.offline.test

/**
  * HF
  * 2020-06-02 21:10
  */
object test2 {
  def main(args: Array[String]): Unit = {

    //var viuMembers = List("a","b","c")
    var viuMembers = List()
    for (i <- 0 until viuMembers.length) {
      if(i == viuMembers.length-1)  println("9999",i)

    }
var a = "是a"
var b = "是b"
    var c = b
    a = b
    c = a

var d  = c
println(d)

  }
}
