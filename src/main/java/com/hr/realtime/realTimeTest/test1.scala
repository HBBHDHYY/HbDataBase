package com.hr.realtime.realTimeTest

import com.alibaba.fastjson.JSON
import com.hr.bean.EtcTollexBillInfo
import com.hr.utils.definitionFunction

/**
  * HF
  * 2020-06-19 12:39
  */
object test1 {
  def main(args: Array[String]): Unit = {


val s = "{\"exVlp\":\"赣C3G807\",\"balanceBefore\":2147457750,\"laneAppVer\":\"2020061401\",\"shift\":\"2\",\"invoiceCnt\":\"0\",\"axisInfo\":\"1000000\",\"paraVer\":\"900002020061713002006170022100200524001240020050500184002006030018900200601002\",\"identification\":2,\"payFee\":2009,\"payType\":4,\"unpayFee\":0,\"enVehicleType\":1,\"cleanDataFlag\":1,\"ticketFee\":0,\"balanceAfter\":2147455850,\"exTollStation\":\"3610401\",\"id\":\"G060N36021001020100302020061808000042\",\"transCode\":\"0201\",\"enLastMoney\":1900,\"overWeightRate\":0,\"vehicleType\":1,\"payCardNet\":\"3601\",\"consumeTime\":212,\"enTollMoney\":2009,\"enTollStationHex\":\"360128A4\",\"spInfo\":\"0\",\"exVehicleId\":\"赣C3G807_0\",\"checkSign\":0,\"exitFeeType\":1,\"axleCount\":2,\"monitorName\":\"ETC操作员\",\"lDate\":\"2020-06-18\",\"exTollStationHex\":\"360128A1\",\"exVehicleType\":1,\"roadType\":1,\"provinceCount\":1,\"cardId\":\"36011936239212102421\",\"transPayType\":1,\"enTollStationName\":\"江西靖安站\",\"cardCnt\":1,\"gantryPassCount\":0,\"exTime\":\"2020-06-18T08:33:17\",\"exTollLaneId\":\"G060N3602100102010030\",\"provinceTransGroup\":\"36:1900\",\"obuProvinceFee\":1900,\"enAxleCount\":2,\"exTollStationName\":\"江西南昌西站\",\"multiProvince\":0,\"splitProvince\":[{\"tollSupport\":\"1\",\"tollIntervalsCount\":0,\"serProvinceId\":\"360201\",\"id\":\"G060N360210010201003020200618080000420\",\"sn\":0,\"modifyFlag\":1,\"tollFee\":1900,\"provinceId\":\"360201\"}],\"overTime\":0,\"cardVersion\":\"16\",\"enVehicleId\":\"赣C3G807_0\",\"unpayCardCost\":0,\"etcCardType\":2,\"obuPayFee\":2009,\"totalCount\":4,\"feeRate\":1.0,\"actualFeeClass\":1,\"vSpeed\":33,\"loginTime\":\"2020-06-18T08:00:02\",\"tollfeeGroup\":\"1900\",\"entryOperatorID\":\"0\",\"feeProvInfo\":0,\"provTransCount\":4,\"payCardTranSN\":120,\"oBUtotalCount\":4,\"exVlpc\":0,\"monitorTime\":\"2020-06-18T08:33:23\",\"verifyCode\":\"0\",\"freeMode\":0,\"obuTotaldisCountAmount\":1900,\"unpayFlag\":\"0\",\"modifyFlag\":1,\"enFreeMoney\":109,\"unifiedFee\":0,\"TAC\":\"FA22D7E8\",\"noCardCount\":0,\"obuSign\":2,\"vCount\":1,\"enWeight\":0,\"serviceType\":1,\"discountFee\":109,\"auth\":\"fangxing\",\"fee\":1900,\"enTollLaneHex\":\"360128A401\",\"freeType\":0,\"tollIntervalsCount\":\"0\",\"obuIssueFlag\":\"江西\",\"spcRateVersion\":\"200601002\",\"inductCnt\":1,\"cardTotalAmount\":1900,\"enTime\":\"2020-06-18T08:08:19\",\"invoiceType\":\"0\",\"vehicleSign\":\"0xff\",\"cardCostFee\":0,\"obuTotalAmount\":2009,\"terminalNo\":\"21360002BF50\",\"eTCtotalAmount\":1900,\"payCardType\":2,\"passId\":\"013601193623921210242120200618080819\",\"realDistance\":40094,\"mediaType\":1,\"rebateMoney\":109,\"shortFeeMileage\":40094,\"enTollLaneId\":\"G060N3602100401010010\",\"limitWeight\":5,\"postBalance\":2147455850,\"bl_Center\":\"3600000\",\"transType\":\"09\",\"terminalTransNo\":\"000156CD\",\"exTollLane\":\"103\",\"identifyVlpc\":0,\"feeMileage\":40094,\"supplierId\":\"3B\",\"signStatus\":\"1\",\"provFee\":1900,\"collectFee\":0,\"operId\":\"1000004\",\"preBalance\":2147457750,\"triggerTime\":\"2020-06-18T08:33:17\",\"obuDiscountFee\":109,\"enVlp\":\"赣C3G807\",\"exVehicleClass\":0,\"payRebate\":0,\"OBUVersion\":\"16\",\"mediaNo\":\"3601190979002430\",\"electricalPercentage\":100,\"laneSpInfo\":\"0001010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"operName\":\"ETC操作员\",\"vehicleClass\":0,\"algorithmIdentifier\":1,\"vehicleId\":\"赣C3G807_0\",\"tollDistance\":40094,\"laneType\":1,\"enVlpc\":0,\"payCardId\":\"1936239212102421\",\"transFee\":1900,\"exTollStationId\":\"G060N360210010\",\"batchNum\":\"G060N3602100102010030202006180800021000004\",\"enTollLane\":\"001\",\"monitor\":\"10003\",\"provinceGroup\":\"360201\",\"receiveTime\":\"2020-06-18 09:36:49.259\",\"bl_SubCenter\":\"3610400\",\"exWeight\":0,\"chargeMode\":1,\"exTollLaneHex\":\"360128A167\",\"enTollStationId\":\"G060N360210040\",\"obuId\":\"3601190979002430\",\"keyNum\":0,\"enTollStation\":\"3610404\",\"enVehicleClass\":0,\"feeBoardPlay\":2,\"shortFee\":1900}"



    val a = JSON.parseObject(s, classOf[EtcTollexBillInfo])
    println(a.balanceBefore) //2147457750
    println(definitionFunction.getCurrentUnixTimestamp())


  }
}
