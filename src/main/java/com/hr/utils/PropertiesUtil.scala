package com.hr.utils
import java.io.InputStream
import java.util.Properties

/**
  * HF
  * 2020-06-05 11:03
  */
object PropertiesUtil {
  private val is: InputStream = ClassLoader.getSystemResourceAsStream("config.properties")
  private val properties = new Properties()
  properties.load(is)
  //传入属性名,,返回属性值
  def getProperty(propertyName: String): String = properties.getProperty(propertyName)

}
