package com.atguigu.gmall.realtime.dws

import com.atguigu.gmall.realtime.BaseAppV3
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetsManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object DwsOrderWideApp extends BaseAppV3{
  override var appName: String = "DwsOrderWideApp"
  override var groupId: String = "DwsOrderWideApp"
  override var topics: Seq[String] = Seq("dwd_order_info","dwd_order_detail")

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: Map[String, DStream[ConsumerRecord[String, String]]]): Unit = {
    //  sourceStream("dwd_order_info").map(_.value()).print(10)
    val orderInfoStream = sourceStream("dwd_order_info").map { record => {
      implicit val f = org.json4s.DefaultFormats
      val orderInfo: OrderInfo = JsonMethods.parse(record.value()).extract[OrderInfo]

        (orderInfo.id,orderInfo)
    }} // 窗口长度，滑动步长
      .window(Seconds(5*1),Seconds(3))
    val orderDetailStream= sourceStream("dwd_order_detail").map{ record => {
      implicit val l = org.json4s.DefaultFormats
      val orderDetail: OrderDetail = JsonMethods.parse(record.value()).extract[OrderDetail]

      (orderDetail.order_id,orderDetail)
    }}.window(Seconds(5*1),Seconds(3))

    // 双流join
    val orderWideStream= orderInfoStream.join(orderDetailStream).map {
      case (_, (orderInfo, orderDetail)) => new OrderWide(orderInfo, orderDetail)  }

    // 3. 对重复 join 的数据借助于redis去重  。order_detail_id
    val orderWideDistinctStream = orderWideStream.mapPartitions(
      orderWideIt => {
      val client: Jedis = RedisUtil.getClient
      val result = orderWideIt.filter(orderWide => {
        val oneOrZero = client.sadd(s"order_join:${orderWide.order_id}", orderWide.order_detail_id.toString)
        client.expire(s"order_join:${orderWide.order_id}", 60) // 给每个 key 单独设置过期时间
        oneOrZero == 1
      })
      client.close()
      result
    })

    orderWideDistinctStream

      .foreachRDD(rdd => {
     // 将dws层数据，写入到kafka中
        rdd.foreachPartition{
          it => {
            val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()
            it.foreach(overWid => {
              implicit  val f = org.json4s.DefaultFormats
              producer.send(new ProducerRecord[String,String]("dws_order_wide",Serialization.write(overWid)))
            })
          }
        }
        OffsetsManager.saveOffsets(offsetRanges,groupId,topics)
      })



  }
}
