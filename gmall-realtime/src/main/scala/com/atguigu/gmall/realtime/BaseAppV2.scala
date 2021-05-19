package com.atguigu.gmall.realtime

import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


/**
 * 一个流，消费多个topic
 */
abstract class BaseAppV2 {
  var appName: String
  var groupId: String // 同时消费多个topic
  var topics:  Seq[String]

  def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[(String,String)])

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    val fromOffsets: Map[TopicPartition, Long] = OffsetsManager.readOffsets(groupId, topics)
    val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]

    val sourceStream = MyKafkaUtil
      .getKafkaStream(ssc, groupId, topics, fromOffsets)
      .transform(rdd => {
        offsetRanges.clear
        val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges ++= newOffsetRanges
        rdd
      })
      // 这里不一样，是因为要获取 topic信息 。供后面使用
      .map(record => (record.topic(),record.value()))
  // 保存的时候，需要offsetRanges ，所以也留着 。
    run(ssc, offsetRanges, sourceStream)

    ssc.start()
    ssc.awaitTermination()
  }
}