package com.hr.offline.test
import java.util.Properties

import com.hr.utils.ConfigurationManager
/**
  * HF
  * 2020-06-08 11:56
  */
object testProperty {
  def main(args: Array[String]): Unit = {
    println(ConfigurationManager.getProperty("bootstrap.servers"))


  }

}
