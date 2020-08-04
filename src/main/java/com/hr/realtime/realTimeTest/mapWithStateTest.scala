package com.hr.realtime.realTimeTest

/**
  * HF
  * 2020-07-11 22:00
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object mapWithStateTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("MapWithState")
      .getOrCreate()

    // 创建一个context，批次间隔为2秒钟，
    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Seconds(3))

    // 设置checkpoint目录
    ssc.checkpoint("hdfs://SC01:8020/user/tmp/cp-20181201-2")

    // 创建一个ReceiverInputDStream，从服务器端的netcat接收数据。
    // 服务器主机名SC01（SC01已在Window上的hosts文件中做了映射，没做映射的则写ip就OK了）,监听端口为6666
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("SC01", 6666)

    // 对接收到的数据进行处理，进行切割，分组形式为(day, 1) (word 1)
    val wordsStream: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1))

    val wordCount: MapWithStateDStream[String, Int, Int, Any] = wordsStream.mapWithState(StateSpec.function(func).timeout(Seconds(30)))

    wordCount.print()

    ssc.start()

    ssc.awaitTermination()
  }

  /**
    * 定义一个函数，该函数有三个类型word: String, option: Option[Int], state: State[Int]
    * 其中word代表统计的单词，option代表的是历史数据（使用option是因为历史数据可能有，也可能没有，如第一次进来的数据就没有历史记录），state代表的是返回的状态
    */
  val func = (word: String, option: Option[Int], state: State[Int]) => {
    if(state.isTimingOut()){
      println(word + "is timeout")
    }else{
      // getOrElse(0)不存在赋初始值为零
      val sum = option.getOrElse(0) + state.getOption().getOrElse(0)
      // 单词和该单词出现的频率/ 获取历史数据，当前值加上上一个批次的该状态的值
      val wordFreq = (word, sum)
      // 更新状态
      state.update(sum)
      wordFreq
    }
  }
}
