package com.atguigu.gmall.realtime.ods

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

import java.io.Serializable
import scala.collection.mutable.ListBuffer





/**
 *
 * canal写到kafka的数据，解析，分流，再写到kafka中
 */
object  BaseDBCanalApp  extends BaseApp{
  override var appName: String = "BaseDBCanalApp"
  override var groupId: String = "BaseDBCanalApp"
  override var topic: String = "gmall_db"

  //  要处理的表
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
                   sourceStream: DStream[ String]): Unit = {
    sourceStream
      // 有可能一个data里有多条数据，所以用map不行，需要用flapMap拆开。原来的一条，可以变成多条。

      .flatMap(record => {
        val j: JValue = JsonMethods.parse(record)
        val data: JValue = j \ "data"
        implicit val f = org.json4s.DefaultFormats
        val tableName = JsonMethods.render(j \ "table").extract[String]
        val operate = JsonMethods.render(j \ "type").extract[String] // insert update ...
        println(tableName)
        //  val children: List[json4s.JValue] = data.children

        data.children.map(child => {
          // 将jvalue转化成字符串   Serialization.write(child) 或者 JsonMethods.compact(JsonMethods.render(child))
          // canal 数据传过来，有意义的是， data，table ，type

          (tableName, operate, JsonMethods.compact(JsonMethods.render(child)))
        })
      })
      .filter {
        case (tableName, operate, content) =>
          // 只发送 ods 需要的表, 删除的动作不要, 内容长度不为空"{}"
          tableNames.contains(tableName) && operate.toLowerCase() != "delete" && content.length > 2
      }
      // 写入到ods层（kafka里）
      .foreachRDD(rdd => {
        rdd.foreachPartition(it => {
          val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
          it.foreach {
            case (tableName, operate, content) =>
              val topic = s"ods_${tableName}"
              if (tableName != "order_info") {
                producer.send(new ProducerRecord[String, String](topic, content))
              } else if (operate.toLowerCase() == "insert") { // 针对 order_info 表, 只保留 insert 数据, update 和 delete 数据不需要
                producer.send(new ProducerRecord[String, String](topic, content))
              }
          }
          producer.close()
        })

        OffsetsManager.saveOffsets(offsetRanges, groupId, topic)
      })
  }




}






