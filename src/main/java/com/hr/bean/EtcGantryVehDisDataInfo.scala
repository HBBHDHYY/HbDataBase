package com.hr.bean

/**
  * HF
  * 2020-06-05 16:44
  */
//ETC门架牌识小时批次汇总上传
case class EtcGantryVehDisDataInfo(
                                picId :String,
                                gantryId :String,
                                gantryHex :String,
                            var picTime :String,
                                gantryOrderNum :String,
                                driveDir :String,
                                cameraNum :String,
                                hourBatchNo :String,
                                shootPosition :String,
                                laneNum :String,
                                vehiclePlate :String,
                                vehicleSpeed :String,
                                identifyType :String,
                                vehicleModel :String,
                                vehicleColor :String,
                                imageSize :String,
                                licenseImageSize :String,
                                binImageSize :String,
                                reliability :String,
                                vehFeatureCode :String,
                                faceFeatureCode :String,
                                verifyCode :String,
                                tradeId :String,
                                matchStatus :String,
                                validStatus :String,
                                dealStatus :String,
                                relatedPicId :String,
                                allRelatedPicId :String,
                                stationDBTime :String,
                                stationDealTime :String,
                                stationValidTime :String,
                                stationMatchTime :String,
                                receiveTime :String
                              ) {

}
