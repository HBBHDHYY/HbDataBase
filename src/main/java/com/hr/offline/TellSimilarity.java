//package com.hr.offline;
//import org.apache.hadoop.hive.ql.exec.UDAF;
//import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
//import org.apache.hadoop.hive.ql.exec.UDAF;
//import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.hive.ql.exec.UDF;
//
//
///**
// * HF
// * 2020-05-13 19:54
// */
//public class TellSimilarity extends UDF {
//
//
//    public   int  minDistance(String str, String target,boolean isDefaultlicense) {
//        int n,m,i,j;
//        if(isDefaultlicense){
//            if (str == "默A00000_9") {return 9 ;}
//        }
//        if(null==str){
//            n=0;
//
//        }else{
//            n = str.length();
//        }
//        if(null==target){
//            m=0;
//        }else{
//            m = target.length();
//        }
//        if (n * m == 0){return n + m;}
//
//        int [][] D = new int[n + 1][m + 1];
//
//
//        for ( i = 0; i < n + 1; i++) {
//            D[i][0] = i;
//        }
//        for ( j = 0; j < m + 1; j++) {
//            D[0][j] = j;
//        }
//        for ( i = 1; i < n + 1; i++) {
//            for ( j = 1; j < m + 1; j++) {
//                if (str.charAt(i - 1) != target.charAt(j - 1)) {  //最后一个字母是否相同
//                    D[i][j] = Math.min(D[i - 1][j] + 1, Math.min(D[i][j - 1] + 1, D[i - 1][j - 1] + 1));
//                }else {
//                    D[i][j] = Math.min(D[i - 1][j] + 1, Math.min(D[i][j - 1] + 1, D[i - 1][j - 1]));
//                }
//            }
//        }
//
//        return D[n][m];
//    }
//
//
//
//}
