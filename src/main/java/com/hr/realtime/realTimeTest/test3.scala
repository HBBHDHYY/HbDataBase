package com.hr.realtime.realTimeTest

/**
  * HF
  * 2020-07-19 19:09
  */
object test3 {
  import java.sql.{Date, Timestamp}
  import java.text.SimpleDateFormat
  import java.time.format.DateTimeFormatter
  import java.time.{LocalDateTime, ZoneId}

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}
  import org.apache.spark.sql.types.DataType
  import org.apache.spark.sql.{Row, SparkSession, functions}
  import org.slf4j.LoggerFactory

  import scala.collection.mutable.ArrayBuffer

  /**
    * Author: Wang Pei
    * Summary:
    *     基于处理时间，用`flatMapGroupsWithState`统计每个分组的PV，并手动维护状态
    */
  object FlapMapGroupsWithState {

   // lazy val logger = LoggerFactory.getLogger(MapGroupsWithState.getClass)

    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().master("local[3]").appName(this.getClass.getSimpleName.replace("$", "")).getOrCreate()
      import spark.implicits._

      // 注册UDF
      spark.udf.register("timezoneToTimestamp", timezoneToTimestamp _)
      // 定义Kafka JSON Schema
      val jsonSchema =
        """{"type":"struct","fields":[{"name":"eventTime","type":"string","nullable":true},{"name":"eventType","type":"string","nullable":true},{"name":"userID","type":"string","nullable":true}]}"""

      // InputTable
      val inputTable = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092")
        .option("subscribe", "test_1")
        .load()

      // ResultTable
      val resultTable = inputTable
        .select(from_json(col("value").cast("string"), DataType.fromJson(jsonSchema)).as("value"))
        .select($"value.*")
        // 增加时间列
        .withColumn("timestamp", functions.callUDF("timezoneToTimestamp", functions.col("eventTime"), lit("yyyy-MM-dd HH:mm:ss"), lit("GMT+8")))
        .filter($"timestamp".isNotNull && $"eventType".isNotNull && $"userID".isNotNull)
        // 基于处理时间，不需要设置Watermark
        // .withWatermark("timestamp", "2 minutes")
        // GroupByKey分组, Key: `分钟,用户ID`
        .groupByKey((row: Row) => {
        val timestamp = row.getAs[Timestamp]("timestamp")
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val currentEventTimeMinute = sdf.format(new Date(timestamp.getTime))
        currentEventTimeMinute + "," + row.getAs[String]("userID")
      })
        // flatMapGroupsWithState
        .flatMapGroupsWithState[(String, Long), (String, String, Long)](OutputMode.Update(), GroupStateTimeout.ProcessingTimeTimeout())((groupKey: String, currentBatchRows: Iterator[Row], groupState: GroupState[(String, Long)]) => {

        println("当前组对应的Key: " + groupKey)
        println("当前ProcessingTime: " + groupState.getCurrentProcessingTimeMs())
        println("当前组的状态是否存在: " + groupState.exists)
        println("当前组的状态是否过期: " + groupState.hasTimedOut)

        var totalValue = 0L

        // 当前组状态已过期，则清除状态
        if (groupState.hasTimedOut) {
          println("清除状态...")
          groupState.remove()

          // 当前组状态已存在，则根据需要处理
        } else if (groupState.exists) {
          println("增量聚合....")
          // 历史值: 从状态中获取
          val historyValue = groupState.get._2
          // 当前值: 从当前组的新数据计算得到
          val currentValue = currentBatchRows.size
          // 总值=历史+当前
          totalValue = historyValue + currentValue

          // 更新状态
          val newState = (groupKey, totalValue)
          groupState.update(newState)

          // 设置状态超时时间为10秒
          groupState.setTimeoutDuration(10 * 1000)

          // 当前组状态不存在，则初始化状态
        } else {
          println("初始化状态...")
          totalValue = currentBatchRows.size
          val initialState = (groupKey, totalValue * 1L)
          groupState.update(initialState)
          groupState.setTimeoutDuration(10 * 1000)
        }

        val output = ArrayBuffer[(String, String, Long)]()
        if (totalValue != 0) {
          val groupKeyArray = groupKey.split(",")
          output.append((groupKeyArray(0), groupKeyArray(1), totalValue))
        }
        output.iterator

      }).toDF("minute", "userID", "pv")

      // Query Start
      val query = resultTable
        .writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("update")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()

      query.awaitTermination()

    }

    /**
      * 带时区的时间转换为Timestamp
      *
      * @param dateTime
      * @param dataTimeFormat
      * @param dataTimeZone
      * @return
      */
    def timezoneToTimestamp(dateTime: String, dataTimeFormat: String, dataTimeZone: String): Timestamp = {
      var output: Timestamp = null
      try {
        if (dateTime != null) {
          val format = DateTimeFormatter.ofPattern(dataTimeFormat)
          val eventTime = LocalDateTime.parse(dateTime, format).atZone(ZoneId.of(dataTimeZone));
          output = new Timestamp(eventTime.toInstant.toEpochMilli)
        }
      } catch {
        case ex: Exception => //logger.error("时间转换异常..." + dateTime, ex)
      }
      output
    }
  }

}
