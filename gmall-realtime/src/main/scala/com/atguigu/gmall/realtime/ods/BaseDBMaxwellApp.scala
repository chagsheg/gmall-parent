package com.atguigu.gmall.realtime.ods

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.util.{MyKafkaUtil,  OffsetsManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.mutable.ListBuffer

/**
 * Author lzc
 * Date 2020/8/26 11:57 下午
 */
object BaseDBMaxwellApp extends BaseApp {
  override var appName: String = "BaseDBMaxwellApp"
  override var groupId: String = "bigdata2"
  override var topic: String = "maxwell_gmall_db"

  val tableNames = List(
    "order_info",
    "order_detail",
    "user_info",
    "base_province",
    "base_category3",
    "sku_info",
    "spu_info",
    "base_trademark")

  override def run(ssc: StreamingContext,
                   offsetRanges: ListBuffer[OffsetRange],
                   sourceStream: DStream[String]): Unit = {
    sourceStream
      .map(record => {
        val j: JValue = JsonMethods.parse(record)
        val data: JValue = j \ "data"
        implicit val f = org.json4s.DefaultFormats
        val tableName = JsonMethods.render(j \ "table").extract[String]
        val operate = JsonMethods.render(j \ "type").extract[String] // insert update ...
        (tableName, operate, Serialization.write(data))
      })
      .filter{
        case (tableName, operate, content) =>
          // 只发送 ods 需要的表, 删除的动作不要, 内容长度不为空"{}"
          tableNames.contains(tableName) && operate != "delete" && content.length > 2
      }
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
          it.foreach {
            case (tableName, operate, content) =>
              val topic = s"ods_${tableName}"
              if (tableName != "order_info") {
                producer.send(new ProducerRecord[String, String](topic, content))
              } else if (operate == "insert") { // 针对 order_info 表, 只保留 insert 数据, update 和 delete 数据不需要
                producer.send(new ProducerRecord[String, String](topic, content))
              }
          }
          producer.close()
        })
        OffsetsManager.saveOffsets(offsetRanges, groupId, topic)
      })
  }
}
