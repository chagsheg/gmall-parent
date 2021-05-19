package com.atguigu.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.jackson.{JsonMethods, Serialization}
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ListBuffer


object OffsetsManager {
  def readOffsetsFromMysql(groupId: String, topic: String): Map[TopicPartition, Long] = {
    val url = "jdbc:mysql://hadoop102:3306/gmall_result?characterEncoding=utf-8&useSSL=false&user=root&password=aaaaaa"
    val sql =
      """
        |select
        | *
        |from ads_offset
        |where topic=? and group_id=?
        |""".stripMargin
    JDBCUtil
      .query(url, sql, List(topic, groupId))
      .map(row => {
        val partitionId = row("partition_id").toString.toInt
        val partitionOffset = row("partition_offset").toString.toLong
        (new TopicPartition(topic, partitionId), partitionOffset)
      })
      .toMap

  }

  def saveOffsets(offsetRanges: ListBuffer[OffsetRange],groupId:String,topic:String): Unit = {
    val client: Jedis = RedisUtil.getClient

val key = "offset:${groupId}:${topic}"
    val listBuffer: ListBuffer[(String, String)] = offsetRanges.map(offsetRange => {
      // redis里的hash 要求都得是字符串
      offsetRange.partition.toString -> offsetRange.untilOffset.toString
    })
    import scala.collection.JavaConverters._
    val filedAndvalue = listBuffer.toMap.asJava
// 这里需要的是java的map集合

    client.hmset(key,filedAndvalue)
    println("topic_partition-> offset: "+ filedAndvalue )
    client.close()
  }

  // 从redis读取上次保存的offsets
  // 消费者组对topic的偏移量

  /**
   * 	key                                  value(hash)
	"offset:groupid:topic"             field           value
                                    		partiton_id     offset
                                    		partiton_id     offset
   */
  def readOffsets(groupId:String,topic:String) = {
    val client: Jedis = RedisUtil.getClient
    val key = "offset:${groupId}:${topic}"
    println("读取开始的offsets")
    import scala.collection.JavaConverters._
  val topiionandOffset = client.hgetAll(key).asScala.map{
   case(partiton,offset) =>
     new TopicPartition(topic,partiton.toInt) -> offset.toLong
 }.toMap
    println("初始偏移量: "+ topiionandOffset)
    client.close()
    topiionandOffset
  }

  /**
   *     key : offset:${groupId}        value(hash)
   *                                    field          value
   *                                    topic          partition+offset
   */
  def saveOffsets(offsetRanges: ListBuffer[OffsetRange],groupId:String,topics:Seq[String]): Unit = {
    val client: Jedis = RedisUtil.getClient
// 这里的key 不能和单个topic相关了 "offset:${groupId}:${topic}" ，所以去掉
    val key = "offset:${groupId}" //一个消费者组消费多个topic
    val filedAndvalue =   offsetRanges.groupBy(_.topic)
        .map{
          case(topic,it: ListBuffer[OffsetRange]) =>
            implicit  val f = org.json4s.DefaultFormats
            val value = it.map(offsetRange => (offsetRange.partition,offsetRange.untilOffset))
          .toMap
            (topic,Serialization.write(value))
        }.asJava


    client.hmset(key,filedAndvalue)
    println("多个topic 一个流： topic_partition-> offset: "+ filedAndvalue )
    client.close()
  }


  def readOffsets(groupId:String,topic:Seq[String]) = {
    val client: Jedis = RedisUtil.getClient
    val key = "offset:${groupId}"
    println("读取开始的offsets")
    import scala.collection.JavaConverters._
    val topiionandOffset = client.hgetAll(key).asScala.flatMap{
      case(topic,partitionOffset) =>
        implicit  val f = org.json4s.DefaultFormats
         JsonMethods.parse(partitionOffset).extract[Map[Int,Long]]
      .map{
          case (partition,offset) => new TopicPartition(topic,partition) -> offset
        }
    }.toMap
    println("初始偏移量: "+ topiionandOffset)
    client.close()
    topiionandOffset
  }


}
