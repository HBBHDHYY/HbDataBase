package com.hr.bean

import java.sql.Timestamp

/**
  * HF
  * 2020-07-13 15:12
  * 车道心跳流水
  */
case class EtcTollHeartBeatInfo(
                                 auth : String ,
                                 id : String ,
                                 tollStationId : String ,  //'收费站号 营改增国标编码'
                                 tollLaneId : String , //'车道号 营改增国标编码'
                                 tollStation : String ,
                                 tollLane : String ,
                                 laneType : String ,
                                 laneSign : String ,
                                 heartBeatTime : String ,
                                 vehicleGreyListVersion : String ,
                                 vehicleGreyListRecTime : String ,
                                 vehicleBlackListVersion : String ,
                                 vehicleBlackListRecTime : String ,
                                 obuBlackListVersion : String ,
                                 obuBlackListRecTime : String ,
                                 cpuBlackListVersion : String ,
                                 cpuBlackListRecTime : String ,
                                 cpcGreyListVersion : String ,
                                 cpcGreyListRecTime : String ,
                                 spcRateVersion : String ,
                                 notenableSpcRateVersion : String ,
                                 notenableSpcRateRecTime : String ,
                                 laneStatus : String ,
                                 rsuStatus : String ,
                                 laneControllerStatus : String ,
                                 serverStatus : String ,
                                 cardReaderStatus : String ,
                                 railerStatus : String ,
                                 payEquipmentStatus : String ,
                                 VPLRStatus : String ,
                                 vehDetectorStatus : String ,
                                 axleDetectorStatus : String ,
                                 lightDetectorStatus : String ,
                                 HDVideoStatus : String ,
                                 feeBoardStatus : String ,
                                 hintsBoardStatus : String ,
                                 tradfficLight1Status : String ,
                                 infoBoardStatus : String ,
                                 entryOverloadStatus : String ,
                                 laneControllerManuID : String ,
                                 sysVer : String ,
                                 opsCode : String ,
                                 opsVer : String ,
                                 RSUManuID : String ,
                                 RSUHardwareVersion : String ,
                                 RSUSoftwareVersion : String ,
                                 cardReaderManuID : String ,
                                 cardReaderVersion : String ,
                                 chargeMode : String ,
                                 VPLRManuID : String ,
                                 receivetime : Timestamp ,
                             var eventTime: Timestamp,    //实际数据没有该字段
                                 year : String ,
                                 month : String ,
                                 day : String ,
                                 hour : String
                               ) {

}
