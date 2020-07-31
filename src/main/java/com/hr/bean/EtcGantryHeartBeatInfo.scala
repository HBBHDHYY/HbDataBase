package com.hr.bean

import java.sql.Timestamp

/**
  * HF
  * 2020-07-22 10:44
  * 门架心跳流水
  */
case class EtcGantryHeartBeatInfo(
                                   receiveTime: String,
                                   chargeUnitId: String,  //门架后台编码
                                   vehicleDetectorHeartbeatList: String,
                                   RSUHeartbeatList: String,
                                   gantryHeartbeatList: String, //前端心跳数组
                                   cleanDataFlag: String,
                                   cameraHeartbeatList: String,
                                   chargeUnitHeartbeatList: String,  //后台心跳数组
                                   heatVersion: String, //状态版本号,5分钟一个版本号
                                   otherHeartbeatList: String,
                               var RSUStatus: String, //实际数据没有该字段
                               var VPLRStatus: String, //实际数据没有该字段
                               var gantryId : String, //门架编号,原始数据没有自己加上的
                               var eventTime: Timestamp   //实际数据没有该字段
                                 ) {

}
