package com.atguigu.gmall.realtime

import org.apache.spark.streaming.dstream.DStream
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, OffsetsManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ListBuffer

/**
 * Author cs
 * 把使用SparkStreaming消费kafka的数据的的一些公共代码抽象到一个抽象类BaseApp, 方便后面使用.
 */
abstract class BaseApp {
  var appName: String
  var groupId: String
  var topic: String

  def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[ String]))

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(3))


    val fromOffsets: Map[TopicPartition, Long] = OffsetsManager.readOffsets(groupId, topic)
    val offsetRanges: ListBuffer[OffsetRange] = ListBuffer.empty[OffsetRange]

    val sourceStream = MyKafkaUtil
      .getKafkaStream(ssc, groupId, topic, fromOffsets)
      .transform(rdd => {
        offsetRanges.clear
        val newOffsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges ++= newOffsetRanges
        rdd
      }).map(_.value())
  // 保存的时候，需要offsetRanges ，所以也留着 。
    run(ssc, offsetRanges, sourceStream)

    ssc.start()
    ssc.awaitTermination()
  }
}