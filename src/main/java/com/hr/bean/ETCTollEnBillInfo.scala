package com.hr.bean

import java.sql.Timestamp

/**
  * HF
  * 2020-06-28 14:44exVlp
  * 入口流水
  */
case class ETCTollEnBillInfo(
                              id: String, // comment '交易流水号 '
                              transCode: String, // comment '交易编码 '
                              bl_SubCenter: String, // comment '所属分中心 '
                              bl_Center: String, // comment '所属中心 '
                              lDate: String, // comment '逻辑日期 '
                              shift: String, // comment '班次 '
                              batchNum: String, // comment '批次号 '
                              loginTime: String, // comment '上班时间 '
                              triggerTime: String, // comment '压线圈时间 '
                              operId: String, // comment '操作员工号 '
                              operName: String, // comment '操作员姓名 '
                              monitor: String, // comment '班长工号 '
                              monitorName: String, // comment '班长名称 '
                              monitorTime: String, // comment '授权时间 '
                              laneAppVer: String, // comment '车道程序版本号 '
                              laneType: String, // comment '入口车道类型 '
                              enTollStation: String, // comment '入口站号 '
                              enTollLane: String, // comment '入口车道号 '
                              enTollStationHex: String, // comment '入口站HEX编码 '
                              enTollLaneHex: String, // comment '入口车道HEX编码 '
                              enTollStationId: String, // comment '入口站号(国标) '
                              enTollLaneId: String, // comment '入口车道号(国标) '
                          var enTime: String, // comment '入口时间 '
                              mediaType: String, // comment '通行介质类型 '
                              obuSign: String, // comment 'OBU单/双片标识 '
                              obuIssueFlag: String, // comment 'OBU发行方标识 '
                              obuId: String, // comment 'OBU编号 '
                              supplierId: String, // comment '厂商编号 '
                              vCount: String, // comment '过车数量 '
                              balanceBefore: String, // comment '交易前余额（分） '
                              transFee: String, // comment '卡面扣费金额 '
                              cardType: String, // comment 'ETC卡类型 '
                              cardNet: String, // comment 'ETC/CPC卡网络号 '
                              cardId: String, // comment 'ETC/CPC卡号  '
                              cardBox: String, // comment '卡盒编号 '
                              cardCount: String, // comment '卡箱卡数 '
                              cardSn: String, // comment 'ETC/CPC卡物理序列号 '
                              cardCnt: String, // comment '通行卡数 '
                              inductCnt: String, // comment '感应车流 '
                              vlp: String, // comment '车牌号 '
                              vlpc: String, // comment '车牌颜色 '
                              identifyVlp: String, // comment '识别车牌号 '
                              identifyVlpc: String, // comment '识别车牌颜色 '
                              vehicleType: String, // comment '收费车型 '
                              vehicleClass: String, // comment '车种 '
                              TAC: String, // comment 'TAC码 '
                              transType: String, // comment '交易类型 '
                              terminalNo: String, // comment '终端机编号 '
                              enWeight: String, // comment '入口重量 '
                              axisInfo: String, // comment '轴组信息 '
                              limitWeight: String, // comment '限载总重(kg) '
                              overWeightRate: String, // comment '超限率 '
                              enAxleCount: String, // comment '入口轴数 '
                              electricalPercentage: String, // comment '电量 '
                              terminalTransNo: String, // comment 'PSAM卡脱机交易序号 '
                              signStatus: String, // comment '标记状态 '
                              description: String, // comment '对交易的文字解释 '
                              paraVer: String, // comment '参数版本 '
                              cardVersion: String, // comment '卡片发行版本 '
                              OBUVersion: String, // comment 'OBU发行版本 '
                              keyNum: String, // comment '按键数量 '
                              keyPressInfo: String, // comment '按键信息 '
                              specialType: String, // comment '部特情类型 '
                              laneSpInfo: String, // comment '车道特情状态 '
                              spInfo: String, // comment '业务分析特情状态 '
                              vehicleSignId: String, // comment '车牌识别流水号 '
                              passId: String, // comment '通行标识ID '
                              vehicleSign: String, // comment '车辆识别标识 '
                              vehicleId: String, // comment '实际车辆车牌号码+颜色 '
                              identifyVehicleId: String, // comment '入口识别车辆 车牌号码+颜 色 '
                              direction: String, // comment '行驶方向 '
                              operationMedia: String, // comment '作业媒介 '
                              chargeMode: String, // comment '计费模式 '
                              receivetime: Timestamp, // comment '数据接收时间 YYYY-MM-DDTHH:mm:ss\
                          var eventTime: Timestamp ,//实际数据没有该字段
                          var statisWindowsTime: String  //sparkstreaming
                            ) {


}