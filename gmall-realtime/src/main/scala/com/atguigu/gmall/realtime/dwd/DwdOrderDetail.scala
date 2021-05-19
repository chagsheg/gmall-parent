package com.atguigu.gmall.realtime.dwd

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.bean.{OrderDetail, SkuInfo}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}

import java.util.Properties
import scala.collection.mutable.ListBuffer

object DwdOrderDetail extends  BaseApp{
  override var appName: String = "DwdOrderDetail"
  override var groupId: String = "DwdOrderDetail"
  override var topic: String = "ods_order_detail"

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[String]): Unit = {
  //  sourceStream.print(100)
  val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._
  sourceStream
    .map(str => {
    implicit  val f = org.json4s.DefaultFormats
      // 这里OrderDetail的某些字段会缺失
    JsonMethods.parse(str).extract[OrderDetail]
  })
    .transform(rdd => {
      val skuRdd = spark
        .read
        .jdbc("jdbc:phoenix:localhost:2181","gmall_sku_info",new Properties())
        .as[SkuInfo]
        .rdd
        .map(sku=> (sku.id,sku))

      rdd
        .map(detail => (detail.sku_id.toString,detail))
        .join(skuRdd)
        .map{
        case (skuId,(detail,sku)) =>
          detail.mergeSkuInfo(sku)
        }
    })
   // .print(100)
    .foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val producer = MyKafkaUtil.getKafkaProducer()
        it.foreach(detail => {
          implicit  val f = org.json4s.DefaultFormats
          producer.send(new  ProducerRecord[String,String]("dwd_order_detail",Serialization.write(detail)))
        })
        producer.close()
      })
      OffsetsManager.saveOffsets(offsetRanges, groupId, topic)
    })
  }
}
