package com.atguigu.gmall.realtime

import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


/**
 * 同时消费多个topic，每个topic单独一个流
 */
abstract class BaseAppV3 {
  var appName: String
  var groupId: String // 同时消费多个topic
  var topics:  Seq[String]

  def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: Map[String, DStream[ConsumerRecord[String, String]]])

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))

   // val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]
    // 有多少个流，搞多少集合  .. tomap 不可变集合，但是 可以改里面的元素
    val offsetRanges =  topics.map(topic => (topic,ListBuffer.empty[OffsetRange])).toMap


// 存储的是多个topic的offset
val offsets = OffsetsManager.readOffsets(groupId, topics)

    val sourceStreams = topics.map(topic => {
      val stream = MyKafkaUtil
        .getKafkaStream(ssc, groupId, topic, offsets.filter(_._1.topic() == topic))
        .transform(rdd => {
           // 这个不能清空了，不然会后面的流会清空前面的流
        //  offsetRanges.clear
          offsetRanges(topic).clear()
          val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
       //    offsetRanges ++= newOffsetRanges
          offsetRanges(topic) ++= newOffsetRanges
          rdd
        })

      (topic,stream)
    }).toMap
    val resultListBuffer: ListBuffer[OffsetRange] = offsetRanges.values.reduce(_ ++ _)

run(ssc,resultListBuffer,sourceStreams)

    ssc.start()
    ssc.awaitTermination()
  }
}