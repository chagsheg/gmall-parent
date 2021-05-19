package com.atguigu.gmall.realtime.ads

import com.atguigu.gmall.realtime.BaseAppV4
import com.atguigu.gmall.realtime.bean.OrderWide
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.JsonMethods
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer

object AdsOrderWideApp extends  BaseAppV4{
  override var appName: String = _
  override var groupId: String = _
  override var topic: String = "dws_order_wide"

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[String]): Unit = {
    // scalalike jdbc 加载配置
    DBs.setup()
    sourceStream.map(str => {
      implicit  val f = org.json4s.DefaultFormats
      val orderWide: OrderWide = JsonMethods.parse(str).extract[OrderWide]

      (orderWide.tm_id,orderWide.final_detail_amount)
    })
      .reduceByKey(_+_)

      .foreachRDD(rdd => {
        val tmAndAmount = rdd.collect()
        tmAndAmount.foreach(println)
        println(offsetRanges)
        if (tmAndAmount.nonEmpty)
        DB.localTx(implicit session => {
          // 写数据到mysql，这里的代码，都会在一个事务执行
          // 插入数据
          val dt = LocalDateTime.now()
          val stat_time = s"${dt.toLocalDate}_${dt.toLocalTime}"
          // 使用批次提交: 批次中的参数 每个 Seq 存储一行 sql 的参数
          val dataBatchParamList: List[Seq[Any]] = tmAndAmount.map {
            case ((tm_id), amount) =>
              Seq(stat_time, tm_id, amount)
          }.toList
          val insertDataSql =
            """
              |insert into tm_amount values(?, ?, ?)
              |""".stripMargin
          SQL(insertDataSql).batch(dataBatchParamList: _*).apply()

          // 插入或更新 offset
          val offsetBatchParamList = offsetRanges.map(offsetRange => {
            Seq(groupId, topic, offsetRange.partition, offsetRange.untilOffset)
          })
          throw new UnsupportedOperationException
          val insertOffsetSql =
            """
              |replace into ads_offset values(?, ?, ?, ?)
              |""".stripMargin
          SQL(insertOffsetSql).batch(offsetBatchParamList: _*).apply()
        })

        //写offset到mysql

      })
  }

  /**
   * offset 保存到mysql里，表的设计：
   *
   *
   * use gmall_result;
   CREATE TABLE `ads_offset` (
  `group_id` varchar(200) NOT NULL,
  `topic` varchar(200) NOT NULL,
  `partition_id` int(11) NOT NULL,
  `partition_offset` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`group_id`,`topic`,`partition_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
   */



}




