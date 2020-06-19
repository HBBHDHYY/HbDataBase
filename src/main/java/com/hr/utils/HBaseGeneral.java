package com.hr.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * HF
 * 2020-05-29 20:39
 */
public class HBaseGeneral {

    /**
     * @param countKey  ⽤于计算分区code的字符串
     * @param suffix    跟在分区code和countKey后的后缀
     * @param regionNum 分区总数
     * @description：获取RowKey 格式示例：rowKey = code + countKey + suffix
     * @author: LiangYe
     * @DATE: 2019/3/27 11:01
     */
    public static String getRowKey( String countKey, String suffix, Integer regionNum) {
        String rowKey;
        if (regionNum > 1) {
//计算regionCode
            String regionCode = getRegionCode(countKey, regionNum);
            rowKey = regionCode + countKey + suffix;
        } else {
            rowKey = countKey + suffix;
        }
        return rowKey;
    }


    /**
     * @param countKey  计算的key
     * @param regionNum 指定分区总数
     * @description：计算regionCode
     * @author: LiangYe
     * @DATE: 2019/3/26 14:33
     */
    public static String getRegionCode(String countKey, Integer regionNum) {
        if (countKey == null || "".equals(countKey)) {
            return null;
        }
        String result = null;
//新表计算⽅法------------------------------------begin
        int regionCode = Math.abs(countKey.hashCode()) % regionNum;
        int codeLength = (regionCode + "").length();//计算得出的分区位数
        int totalLength;//分区总数位数
        if (regionNum.intValue() <= 10) {
            totalLength = 2;
        } else if (100 == regionNum.intValue()) {//100分区实际是00-99的两位数分区总数
            totalLength = 2;
        } else if (1000 == regionNum.intValue()) {//1000分区实际是000-999的三位数分区总数
            totalLength = 3;
        } else {//其他情况按照传⼊的分区总数算
            totalLength = (regionNum + "").length();
        }
        for (int i = 0; i < totalLength - codeLength; i++) {
            if (result == null) {
                result = "0" + regionCode;
            } else {
                result = "0" + result;
            }
        }
        if (result == null) {
            result = regionCode + "";
        }
//新表计算⽅法------------------------------------end
        return result;
    }

    public static String dateToStr(java.sql.Timestamp time) {
        String strFormat = "yyyy-MM-dd HH:mm:ss";
        DateFormat df = new SimpleDateFormat(strFormat);
        String str = df.format(time);
        return str;}
}
