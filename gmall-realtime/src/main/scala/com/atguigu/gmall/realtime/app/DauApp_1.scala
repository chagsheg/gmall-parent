package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.app.DauAppAuto.{distinct2, parseToStartupLog}
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{EsUtil, MyKafkaUtil, OffsetsManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

import java.time.LocalDate
import scala.collection.mutable.ListBuffer
// 自动提交会导致数据重复消费的原因 ：  自动提交 默认是 几秒中 保存一次，  在已经消费，但未保存时挂掉，会重复消费。（可以用redis去重）
/**
 * 手动保存偏移量的核心，就是先保存到数据库，保存offset。(保证数据不丢失，可能重复) 。 再利用数据库的幂等性保证数据不重复。
 */

/**
 * 	key                                  value(hash)
	"offset:groupid:topic"             field           value
                                    		partiton_id     offset
                                    		partiton_id     offset
 */
object DauApp_1 {

val groupId = "DauApp"
val topic = "gmall_startup_topic"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    val offsetRanges = ListBuffer.empty[OffsetRange]

    // 1. 从redis读取上次保存的offsets 。启动的时候，读一次就行了。
    val offsets =OffsetsManager.readOffsets(groupId,topic)

    // ConsumerRecord 里面有这次消费到哪，offset的信息
    val sourceStream =
      MyKafkaUtil.getKafkaStream(ssc, groupId, topic,offsets)
        // sourceStream.map(_.value()).print()
        // 每3s的数据，封装到一个rdd里面，此时我们需要拿到rdd，从rdd中，拿到想要的消费到的offset的位置
        .transform(rdd => {
        //读取到这次消费的offsets
        // 这个强转，必须是从kafka直接得到的那个流中的rdd.也即不能对流.map的原因
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //一个OffsetRange 是一个分区， offsetRanges 的长度应该是 分区数。 但是这个代码是 每3秒执行一次，所以如果不清理，他的长度会一直增加。
        offsetRanges.clear()
        offsetRanges ++= ranges
        rdd
      })
         .map(_.value())
    /**
     * 这里验证 sourceStream 的话，可以去掉后面的 .map(_.value()) ,
     * sourceStream.foreachRDD((rdd) => {       rdd.map(_.value()).collect().foreach(println)  }
     */



 val startupLogStream = parseToStartupLog(sourceStream)
   val result = distinct2(startupLogStream)

    //2. 保存offsets应该每个批次保存一次。 根据前面的分析，是先写数据，再保存offsets
 result.foreachRDD((rdd: RDD[StartupLog]) => {


      // 2.1 先写到es
   rdd.foreachPartition(it =>{ val today: String = LocalDate.now.toString
     // 用id 保证数据不重复
 EsUtil.insertBulk(s"gmall_dau_info_$today",it.map(log => (log.mid,log)))})



      // 2.2 再 保存offsets到redis  ? 如何知道这次消费到了哪些个offsets .  ConsumerRecord 这个有
OffsetsManager.saveOffsets(offsetRanges,groupId, topic)


    })

    ssc.start()
    ssc.awaitTermination()
  }

  private def parseToStartupLog(sourceStream: DStream[String]) = {
    sourceStream.map(jsonString => {
      val j: JValue = JsonMethods.parse(jsonString)
      val jCommon = j \ "common"
      val jTs = j \ "ts"
      implicit val d = org.json4s.DefaultFormats
      jCommon.merge(JObject("ts" -> jTs)).extract[StartupLog]
    })
  }


  /**
   * 按照 mid, 对流的数据进行去重
   * 优化后
   *
   * @param startupLogStream
   * @return
   */
  def distinct2(startupLogStream: DStream[StartupLog]) = {

    startupLogStream.mapPartitions(startupLogIt => {
      val client: Jedis = RedisUtil.getClient
      val result = startupLogIt.filter(startupLog => {
        val key = "mids7:" + startupLog.logDate
        val result = client.sadd(key, startupLog.mid)
        client.close()
        result == 1
      })
      client.close()
      result
    })
  }
















}