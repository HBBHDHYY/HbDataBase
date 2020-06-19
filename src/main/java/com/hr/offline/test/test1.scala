package com.hr.offline.test

/**
  * HF
  * 2020-06-01 22:18
  */
object test1 {
  def main(args: Array[String]): Unit = {

    var results = List[(String, String, String, String, String, String, Int, Int)]()
    results :+= ("1", "1", "1", "1", "1", "1", 1, 1)
    println(results)

    results :+= ("1", "1", "1", "1", "1", "1", 1, 2)
    println(results)

    results :+= ("1", "1", "1", "1", "1", "1", 1, 3)
    println(results)
    import scala.util.control.Breaks
    Breaks.breakable(
      for (i <- 1 to 10) {
        for (j <- 1 to 10){
          if (i >= 5) {
            println("wocao")
            Breaks.break()
          }
        }
      }
    )
    println("循环结束了")

  }

}
