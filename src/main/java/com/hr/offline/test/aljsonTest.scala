package com.hr.offline.test

import com.alibaba.fastjson.JSON
import com.hr.bean.EtcTollexBillInfo

/**
  * HF
  * 2020-06-21 22:17
  */
object aljsonTest {
  def main(args: Array[String]): Unit = {
    var value = "{\"exVlp\":\"赣C37530\",\"balanceBefore\":4294845492,\"laneAppVer\":\"2020061401\",\"shift\":\"1\",\"invoiceCnt\":\"0\",\"axisInfo\":\"1000000\",\"paraVer\":\"900002020061713002006170022100200524001240020050500184002006030018900200601002\",\"identification\":2,\"payFee\":1998,\"payType\":4,\"unpayFee\":0,\"enVehicleType\":1,\"cleanDataFlag\":1,\"ticketFee\":0,\"balanceAfter\":4294843594,\"exTollStation\":\"3610401\",\"id\":\"G060N36021001020100302020061807000042\",\"transCode\":\"0201\",\"enLastMoney\":1898,\"overWeightRate\":0,\"vehicleType\":1,\"payCardNet\":\"3601\",\"consumeTime\":188,\"enTollMoney\":1998,\"enTollStationHex\":\"360128A3\",\"spInfo\":\"0\",\"exVehicleId\":\"赣C37530_0\",\"checkSign\":0,\"exitFeeType\":1,\"axleCount\":2,\"monitorName\":\"ETC操作员\",\"lDate\":\"2020-06-18\",\"exTollStationHex\":\"360128A1\",\"exVehicleType\":1,\"roadType\":1,\"provinceCount\":1,\"cardId\":\"36011601231104046683\",\"transPayType\":1,\"enTollStationName\":\"江西奉新站\",\"cardCnt\":1,\"gantryPassCount\":0,\"exTime\":\"2020-08-18T07:50:29\",\"exTollLaneId\":\"G060N3602100102010030\",\"provinceTransGroup\":\"36:1898\",\"obuProvinceFee\":1898,\"enAxleCount\":2,\"exTollStationName\":\"江西南昌西站\",\"multiProvince\":0,\"splitProvince\":[{\"tollSupport\":\"1\",\"tollIntervalsCount\":0,\"serProvinceId\":\"360201\",\"id\":\"G060N360210010201003020200618070000420\",\"sn\":0,\"modifyFlag\":1,\"tollFee\":1898,\"provinceId\":\"360201\"}],\"overTime\":0,\"cardVersion\":\"16\",\"enVehicleId\":\"赣C37530_0\",\"unpayCardCost\":0,\"etcCardType\":2,\"obuPayFee\":1998,\"totalCount\":4,\"feeRate\":1.0,\"actualFeeClass\":1,\"vSpeed\":25,\"loginTime\":\"2020-06-18T00:00:04\",\"tollfeeGroup\":\"1898\",\"entryOperatorID\":\"0\",\"feeProvInfo\":0,\"provTransCount\":4,\"payCardTranSN\":249,\"oBUtotalCount\":4,\"exVlpc\":0,\"monitorTime\":\"2020-06-18T07:50:35\",\"verifyCode\":\"0\",\"freeMode\":0,\"obuTotaldisCountAmount\":1898,\"unpayFlag\":\"0\",\"modifyFlag\":1,\"enFreeMoney\":100,\"unifiedFee\":0,\"TAC\":\"57E03ECE\",\"noCardCount\":0,\"obuSign\":2,\"vCount\":1,\"enWeight\":0,\"serviceType\":1,\"discountFee\":100,\"auth\":\"fangxing\",\"fee\":1898,\"enTollLaneHex\":\"360128A301\",\"freeType\":0,\"tollIntervalsCount\":\"0\",\"obuIssueFlag\":\"江西\",\"spcRateVersion\":\"200601002\",\"inductCnt\":1,\"cardTotalAmount\":1898,\"enTime\":\"2020-06-18T07:27:52\",\"invoiceType\":\"0\",\"vehicleSign\":\"0xff\",\"cardCostFee\":0,\"obuTotalAmount\":1998,\"terminalNo\":\"21360002BF50\",\"eTCtotalAmount\":1898,\"payCardType\":2,\"passId\":\"013601160123110404668320200618072752\",\"realDistance\":40114,\"mediaType\":1,\"rebateMoney\":100,\"shortFeeMileage\":40114,\"enTollLaneId\":\"G060N3602100301010010\",\"limitWeight\":0,\"postBalance\":4294843594,\"bl_Center\":\"3600000\",\"transType\":\"09\",\"terminalTransNo\":\"0001569D\",\"exTollLane\":\"103\",\"identifyVlpc\":0,\"feeMileage\":40114,\"supplierId\":\"3B\",\"signStatus\":\"1\",\"provFee\":1898,\"collectFee\":0,\"operId\":\"1000004\",\"preBalance\":4294845492,\"triggerTime\":\"2020-06-18T07:50:29\",\"obuDiscountFee\":100,\"enVlp\":\"赣C37530\",\"exVehicleClass\":0,\"payRebate\":0,\"OBUVersion\":\"16\",\"mediaNo\":\"3601190808027857\",\"electricalPercentage\":100,\"laneSpInfo\":\"0001010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000\",\"operName\":\"ETC操作员\",\"vehicleClass\":0,\"algorithmIdentifier\":1,\"vehicleId\":\"赣C37530_0\",\"tollDistance\":40114,\"laneType\":1,\"enVlpc\":0,\"payCardId\":\"1601231104046683\",\"transFee\":1898,\"exTollStationId\":\"G060N360210010\",\"batchNum\":\"G060N3602100102010030202006180000041000004\",\"enTollLane\":\"001\",\"monitor\":\"10003\",\"provinceGroup\":\"360201\",\"receiveTime\":\"2020-06-18 09:36:49.259\",\"bl_SubCenter\":\"3610400\",\"exWeight\":0,\"chargeMode\":1,\"exTollLaneHex\":\"360128A167\",\"enTollStationId\":\"G060N360210030\",\"obuId\":\"3601190808027857\",\"keyNum\":0,\"enTollStation\":\"3610403\",\"enVehicleClass\":0,\"feeBoardPlay\":2,\"shortFee\":1898}"


    val result: EtcTollexBillInfo = JSON.parseObject(value, classOf[EtcTollexBillInfo])

    println(result.mediaType)






  }
}
