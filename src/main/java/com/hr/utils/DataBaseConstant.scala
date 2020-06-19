package com.hr.utils

/**
  * HF
  * 2020-06-05 11:15
  */
object DataBaseConstant {
//消费的Kafka主题,江西和河北
  val TOPIC_ETC: String = "etcData"
  val TOPIC_VIU: String = "viuData"

  val TOPIC_ETC_TollEn_BillInfo: String = "ETC_tollEnBillInfo" //车道入口交易数据
  val TOPIC_ETC_TollEx_BillInfo: String = "etc_tollexbillinfo" //车道出口交易数据
  val TOPIC_ETC_issuePosOperateWaste: String = "ETC_issuePosOperateWaste" //赣通卡操作日志表




  //要写入的的Kafka主题,江西和河北
  val TOPIC_RESULT: String = "etcVIUResultData"

  val TOPIC_etc_tollexbillinfo_result: String = "etc_tollexbillinfo_result"


}
