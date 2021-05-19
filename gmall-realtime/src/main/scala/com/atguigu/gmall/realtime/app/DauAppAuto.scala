package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{EsUtil, MyKafkaUtil, RedisUtil}

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.{JObject, JValue}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

import java.time.LocalDate

object DauAppAuto {



  def main(args: Array[String]): Unit = {
    // 1. 创建一个StreamingContext

    val conf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 2. 从kafka中获取输入流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, "gmall_startup_topic")
    //3将字符串的类型，解析成样例类
// json4s 转换
    val startupLogStream = parseToStartupLog(sourceStream)
    // 去重
    // val result: DStream[StartupLog] = distinct(startupLogStream)
  val result = distinct2(startupLogStream)


    //  流要输出 print , foreachRDD
    //，是保证数据不重复。  可以用redis去重，也可以用 es去重。 2中思路，都ok 。
      result.foreachRDD(rdd => {
        import EsUtil._
        val today: String = LocalDate.now.toString
     rdd.saveToEs(s"gmall_dau_info_$today")

      })



 // startupLogStream.print()
    //  sourceStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 把流中的字符串解析为样例类 StartupLog 类型
   *
   * @param sourceStream
   * @return34；；p
   */
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
  def distinct(startupLogStream: DStream[StartupLog]) = {
    val preKey = "mids:"
    startupLogStream.filter(startupLog => {
      val client: Jedis = RedisUtil.getClient
      val key = preKey + startupLog.logDate
      // 把 mid 写入到 Set 中, 如果返回值为 1 表示写入成功, 返回值为 0 表示不是首次写入, 数据重复
      val result = client.sadd(key, startupLog.mid)
      client.close()
      result == 1 // 首次写入的保留下来, 非首次写入的过滤掉
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
       // 使用transform，性能和2一样.
  def distinct3(startupLogStream: DStream[StartupLog]) =  {
// 代码1 driver只执行1次
    startupLogStream.transform(rdd => {
      // 代码2 driver 每批次执行一次
      rdd.mapPartitions(startupLogIt => {
        val client: Jedis = RedisUtil.getClient
        // 代码3 executer 每个批次每个分区执行一次 ，
        val result = startupLogIt.filter(startupLog => {
          val key = "mids3:" + startupLog.logDate
          val result = client.sadd(key, startupLog.mid)
          client.close()
          result == 1
        })
        client.close()
        result
      })


    })



  }
  /**
   * 把数据写入到 es 中
   *
   * @param stream
   * @return
   */
  def saveToES(stream: DStream[StartupLog]) = {
    stream.foreachRDD(rdd => {
      rdd.foreachPartition(startupLogIt => {
        val today: String = LocalDate.now.toString
        // 比如使用gmall_dau_info_2020-08-20 作为 index
        EsUtil.insertBulk(s"gmall_dau_info_$today", startupLogIt)
      })
    })
  }













}