package com.hr.offline.test

/**
  * HF
  * 2020-06-13 17:09
  */
object test5 {
  def main(args: Array[String]): Unit = {

    var (year, month, day, hour, day_or_hour, taskDescription) =(2020,"05",13,23,"hour", "test")

    val originTableName= "spark_test.etc_tollexbillinfo"  //测试表名
    val resultTableName= "spark_test.ETC_exConsumeDailyStatistic"  //测试表名
    //测试使用hour
    val exitStationTatisticsSql_supplement = if (day_or_hour == "day") s"and `hour`='${hour}'" else ""

    val exitStationTatisticsSql =
      s"""
         |select
         |extollstationid,--comment  '出口站编码(国标)'
         |exTollStationName ,--comment '出口收费站名称'
         |lDate    statisDay , --comment 当做事件发生时间
         |sum(case when mediatype = 1 and exvehicletype = 1 then 1 else 0 end)  etc_1_VehicleType, --comment 'etc各个车型'etcTypeCount
         |sum(case when mediatype = 1 and exvehicletype = 2 then 1 else 0 end)  etc_2_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 3 then 1 else 0 end)  etc_3_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 4 then 1 else 0 end)  etc_4_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 11 then 1 else 0 end) etc_11_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 12 then 1 else 0 end) etc_12_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 13 then 1 else 0 end) etc_13_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 14 then 1 else 0 end) etc_14_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 15 then 1 else 0 end) etc_15_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 16 then 1 else 0 end) etc_16_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 21 then 1 else 0 end) etc_21_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 22 then 1 else 0 end) etc_23_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 23 then 1 else 0 end) etc_23_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 24 then 1 else 0 end) etc_24_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 25 then 1 else 0 end) etc_25_VehicleType,
         |sum(case when mediatype = 1 and exvehicletype = 26 then 1 else 0 end) etc_26_VehicleType,
         |
 |sum(case when mediatype = 1 and exvehicleclass = 0 then 1 else 0 end)  etc_0_VehicleClass, --comment 'etc各个车种'etcClassCount
         |sum(case when mediatype = 1 and exvehicleclass = 8 then 1 else 0 end)  etc_8_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 10 then 1 else 0 end) etc_10_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 14 then 1 else 0 end) etc_14_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 21 then 1 else 0 end) etc_21_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 22 then 1 else 0 end) etc_22_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 23 then 1 else 0 end) etc_23_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 24 then 1 else 0 end) etc_24_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 25 then 1 else 0 end) etc_25_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 26 then 1 else 0 end) etc_26_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 27 then 1 else 0 end) etc_27_VehicleClass,
         |sum(case when mediatype = 1 and exvehicleclass = 28 then 1 else 0 end) etc_28_VehicleClass,
         |
 |sum(case when mediatype =1 and signstatus = 1 then 1 else 0 end) as etcSuccessCount, --comment 'etc成功处理'
         |sum(case when mediatype =1 and signstatus = 2 then 1 else 0 end) as etcFailCount, --comment 'etcetc失败处理'
         |
 |sum(case when mediatype = 2 and exvehicletype = 1 then 1 else 0 end)  cpc_1_VehicleType,--comment 'cpc各个车型的'cpcTypeCount
         |sum(case when mediatype = 2 and exvehicletype = 2 then 1 else 0 end)  cpc_2_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 3 then 1 else 0 end)  cpc_3_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 4 then 1 else 0 end)  cpc_4_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 11 then 1 else 0 end) cpc_11_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 12 then 1 else 0 end) cpc_12_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 13 then 1 else 0 end) cpc_13_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 14 then 1 else 0 end) cpc_14_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 15 then 1 else 0 end) cpc_15_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 16 then 1 else 0 end) cpc_16_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 21 then 1 else 0 end) cpc_21_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 22 then 1 else 0 end) cpc_22_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 23 then 1 else 0 end) cpc_23_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 24 then 1 else 0 end) cpc_24_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 25 then 1 else 0 end) cpc_25_VehicleType,
         |sum(case when mediatype = 2 and exvehicletype = 26 then 1 else 0 end) cpc_26_VehicleType,
         |
 |sum(case when mediatype = 2 and exvehicleclass = 0 then 1 else 0 end)  cpc_0_VehicleClass,--comment 'cpc各个车种的'cpcClassCount
         |sum(case when mediatype = 2 and exvehicleclass = 8 then 1 else 0 end)  cpc_8_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 10 then 1 else 0 end) cpc_10_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 14 then 1 else 0 end) cpc_14_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 21 then 1 else 0 end) cpc_21_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 22 then 1 else 0 end) cpc_22_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 23 then 1 else 0 end) cpc_23_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 24 then 1 else 0 end) cpc_24_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 25 then 1 else 0 end) cpc_25_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 26 then 1 else 0 end) cpc_26_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 27 then 1 else 0 end) cpc_27_VehicleClass,
         |sum(case when mediatype = 2 and exvehicleclass = 28 then 1 else 0 end) cpc_28_VehicleClass,
         |
 |sum(case when mediatype =2 and signstatus = 1 then 1 else 0 end) as cpcSuccessCount, --comment cpc成功处理的
         |sum(case when mediatype =2 and signstatus = 1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as cpcSuccessFee, --comment cpc成功处理的总金额
         |sum(case when mediatype =2 and signstatus = 2 then 1 else 0 end) as cpcFailCount, --comment cpc失败处理的
         |sum(case when mediatype =2 and signstatus = 2 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as cpcFailFee,--comment cpc失败处理的总金额
         |
 |sum(case when mediatype =3 and signstatus = 1 then 1 else 0 end) as paperSuccessCount, --comment 纸券成功交易量
         |sum(case when mediatype =3 and signstatus = 1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as pagerIssueSuccessSumFee, --comment纸券成功交易额
         |sum(case when mediatype =3 and signstatus = 2 then 1 else 0 end) as paperFailCount ,--comment 纸券失败交易量
         |sum(case when mediatype =3 and signstatus = 2 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as paperFailFee ,--comment 纸券失败交易额
         |count(1) as totalVeCnt ,--comment 总车流量
         |sum(cast(nvl(fee,0) as decimal(10,4))) as totalTransFee ,--comment 总金额,
         |sum(case when mediaType=1 then 1 else 0 end) as etcCnt,--comment ETC总量
         |sum(case when mediatype =1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as etcTotalDiscountFee,--comment ETC总交易金额
         |sum(cast(nvl(discountFee,0) as decimal(10,4))) as totalDiscountFee,--comment 总优惠金额
         |
 |sum(case when mediatype =9 then 1 else 0 end) as noMediaTypeTotalNum,--comment 无通行介质总数
         |sum(case when mediatype =4 then 1 else 0 end) as m1PassNum,--comment M1通行总数
         |sum(case when multiProvince =0 then 1 else 0 end) as singleProTransTotalNum,--comment 单省交易总数
         |sum(case when multiProvince =0 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as singleProTransTotalFee,--comment 单省交易金额
         |sum(case when multiProvince =1 then 1 else 0 end) as mutilProTransTotalNum,--comment 多省交易总数
         |sum(case when multiProvince =1 then cast(nvl(fee,0) as decimal(10,4)) else 0 end) as mutilProTransTotalFee,--comment 多省交易金额
         |
 |sum(case when payType= 1  then 1 else 0 end)  1_payTypeCount, --comment 各个支付类型总个数payTypeTotalNum
         |sum(case when payType= 2  then 1 else 0 end)  2_payTypeCount,
         |sum(case when payType= 3  then 1 else 0 end)  3_payTypeCount,
         |sum(case when payType= 4  then 1 else 0 end)  4_payTypeCount,
         |sum(case when payType= 5  then 1 else 0 end)  5_payTypeCount,
         |sum(case when payType= 6  then 1 else 0 end)  6_payTypeCount,
         |sum(case when payType= 7  then 1 else 0 end)  7_payTypeCount,
         |
 |sum(case when payType= 1  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  1_payTypeSum, --comment 各个支付类型总金额payTypeTotalFee
         |sum(case when payType= 2  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  2_payTypeSum,
         |sum(case when payType= 3  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  3_payTypeSum,
         |sum(case when payType= 4  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  4_payTypeSum,
         |sum(case when payType= 5  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  5_payTypeSum,
         |sum(case when payType= 6  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  6_payTypeSum,
         |sum(case when payType= 7  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  7_payTypeSum,
         |
 |sum(case when actualFeeClass= 1  then 1 else 0 end)  1_actualFeeClassCount, --comment 实际计费方式总个数actualWayTotalNum
         |sum(case when actualFeeClass= 2  then 1 else 0 end)  2_actualFeeClassCount,
         |sum(case when actualFeeClass= 3  then 1 else 0 end)  3_actualFeeClassCount,
         |sum(case when actualFeeClass= 4  then 1 else 0 end)  4_actualFeeClassCount,
         |sum(case when actualFeeClass= 5  then 1 else 0 end)  5_actualFeeClassCount,
         |sum(case when actualFeeClass= 6  then 1 else 0 end)  6_actualFeeClassCount,
         |sum(case when actualFeeClass= 11  then 1 else 0 end) 11_actualFeeClassCount,
         |
 |sum(case when payType= 1  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  1_actualFeeClassSum, --comment 实际计费方式总金额actualWayTotalFee
         |sum(case when payType= 2  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  2_actualFeeClassSum,
         |sum(case when payType= 3  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  3_actualFeeClassSum,
         |sum(case when payType= 4  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  4_actualFeeClassSum,
         |sum(case when payType= 5  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  5_actualFeeClassSum,
         |sum(case when payType= 6  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  6_actualFeeClassSum,
         |sum(case when payType= 11  then cast(nvl(fee,0) as decimal(10,4)) else 0 end)  11_actualFeeClassSum
         |
 |from  ${originTableName}
         |where `year`='${year}' and `month`='${month}' and `day`='${day}' ${exitStationTatisticsSql_supplement}
         |group by extollstationid
       """.stripMargin


    println(exitStationTatisticsSql)


  }
}
