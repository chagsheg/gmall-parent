package com.atguigu.gmall.realtime.util

import org.apache.spark.sql.{Encoder, SparkSession}

object SparkSqlUtil {
  val url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
 // 从sparksql 读取的数据，转成rdd . sparkSession 需要倒入sparksql 依赖

   // df => ds => rdd
  def getRDD[C: Encoder](spark: SparkSession, sql: String) = {
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("query", sql)
      .load()
      .as[C]
      .rdd
  }
}