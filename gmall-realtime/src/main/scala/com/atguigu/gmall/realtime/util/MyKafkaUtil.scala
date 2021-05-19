package com.atguigu.gmall.realtime.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable



object MyKafkaUtil {

  var kafkaParams = Map[String, Object](
    "bootstrap.servers" -> ConfigUtil.getProperty("kafka.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> ConfigUtil.getProperty("kafka.group"),
    /*
    earliest
    当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
      latest
      当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
      none
      topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */
    "auto.offset.reset" -> "latest", // 如果有保存 offset, 则从保存位置开始消费, 没有则从latest开始消费
    "enable.auto.commit" -> (true: java.lang.Boolean),
    // 生产者配幂等性
      "enable.idempotence" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(ssc: StreamingContext, topic: String ) = {
    KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](Set(topic), kafkaParams))
    .map(_.value)
  }

// 手动维护的代码， 返回值和上面的不一样，得到的值包含offset相关信息
  def getKafkaStream(ssc: StreamingContext,
                     groupId:String,
                      topic: String,
                      offsets:Map[TopicPartition,Long]) = {
 // 手动维护
   kafkaParams += "auto.offset.reset" -> "earliest"
   kafkaParams += "enable.auto.commit" -> (false: java.lang.Boolean)
    kafkaParams += "group.id" -> groupId
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Set(topic), kafkaParams,offsets)
    )
    //因为需要有offset相关的信息，所以不能和上面一样，不需要map(_.value) 函数了，

  }

  def getKafkaProducer() = {
    import scala.collection.JavaConverters._
    new KafkaProducer[String, String](kafkaParams.asJava)
  }
  // 一个流，消费多个topic
  def getKafkaStream(ssc: StreamingContext,
                     groupId:String,
                     topics: Seq[String],
                     offsets:Map[TopicPartition,Long]) = {
    // 手动维护
    kafkaParams += "auto.offset.reset" -> "earliest"
    kafkaParams += "enable.auto.commit" -> (false: java.lang.Boolean)
    kafkaParams += "group.id" -> groupId
    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams,offsets)
    )
    //因为需要有offset相关的信息，所以不能和上面一样，不需要map(_.value) 函数了，

  }

}