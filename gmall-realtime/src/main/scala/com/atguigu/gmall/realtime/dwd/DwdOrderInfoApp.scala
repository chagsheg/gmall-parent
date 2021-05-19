package com.atguigu.gmall.realtime.dwd

import com.atguigu.gmall.realtime.BaseApp
import com.atguigu.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserInfo, UserStatus}
import com.atguigu.gmall.realtime.util.{EsUtil, MyKafkaUtil, OffsetsManager, SparkSqlUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.JsonAST.JDouble
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{CustomSerializer, JInt, JLong, JString}

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

/**
 * 经过过滤，只留用户的首单，将首单信息写入到es中。
 */
object DwdOrderInfoApp  extends BaseApp{
  override var appName: String = "DwdOrderInfoApp"
  override var groupId: String = "DwdOrderInfoApp"
  override var topic: String = "ods_order_info"

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[String]): Unit = {

    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    import  spark.implicits._
    //  现将json字符串封装到类中
    var orderInfoStream = sourceStream.map(
      str => {
        /** 正常情况下这样写就ok了。 但是由于我这里学习，所以导致kafka流中的数据，canal和maxwell 都有。
         * maxwell的格式不变，而canal会把数据库的 string int等 乱转换。所以需要自定义序列化器。
         *     println(str)
        val string2Long = new CustomSerializer[Long]( format =>(
          {
            case JString(s) => s.toLong
            case JInt(s) => s.toLong
          },{
          case s:Long => JLong(s)
        }))
        val string2Double = new CustomSerializer[Double]( format =>(
          {
            case JString(s) => s.toLong
           case JDouble(s) => s.toDouble
          },{
          case s:Double => JDouble(s)
        }))
        implicit val f = org.json4s.DefaultFormats + string2Long
         */
        implicit val f = org.json4s.DefaultFormats
        // 实时表
        JsonMethods.parse(str).extract[OrderInfo]
      }
    )

    // 1. 补充纬度表信息 。  通过sparkSql去读，读到的是df，df和rdd 做join就可以了。流是没办法写sparksql的，所以需要先转换。
  val  orderInfoStreamWithAll =   orderInfoStream.transform{
      // 这个rdd没有维度信息
      rdd => {
rdd.cache()
      val ids =  rdd.map(_.user_id).collect().mkString("','")  // 1 2 3 ==>  1','2','3
        // 1.  读取维度
   val sql = s"select * from gamll_user_info where id in('${ids}')"
        val userInfoRDD = SparkSqlUtil.getRDD[UserInfo](spark, sql).map(user => (user.id,user))

        val provinceInfoRDD= SparkSqlUtil.getRDD[ProvinceInfo](spark, sql).map(pro => (pro.id,pro))
        // 2.  rdd join维度，返回join后的rdd . join需要两边都是key，value 类型

        rdd.map(info => (info.user_id.toString, info))
          .join(userInfoRDD)
          .map {
            case (user_id, (orderInfo, userInfo)) =>
              orderInfo.user_age_group = userInfo.age_group
              orderInfo.user_gender = userInfo.gender_name
              (orderInfo.province_id.toString,orderInfo)
          }
          .join(provinceInfoRDD)
          .map {
            case (province_id, (orderInfo, provinceInfo)) =>
              orderInfo.province_name = provinceInfo.name
              orderInfo.province_area_code = provinceInfo.area_code
              orderInfo.province_iso_code = provinceInfo.iso_code
              orderInfo
          }
      }
    }

    // 2.  处理首单
    val resultStream = orderInfoStreamWithAll.transform {
      rdd => {
        rdd.cache()
        val str = rdd.map(_.user_id).collect().mkString("','")
        val sql = s"select * from userstatus where id in ('$str')"
        // 其实可以不用status，因为表里存的，都是首单的 。
        val oleUserIds = SparkSqlUtil.getRDD[UserStatus](spark, sql).map(_.user_id).collect()

        rdd.map(orderInfo => {
          if (oleUserIds.contains(orderInfo.user_id.toString)) {
            orderInfo.is_first_order = false
          } else {
            orderInfo.is_first_order = true
          }
       //  orderInfo
      (orderInfo.user_id,orderInfo)
        })
        .groupByKey()
          // 可以先写map试一下，发现返回迭代器， 故 变成faltmap
          .flatMap{
            case (userId,it) =>
              val list = it.toList
              if (list.head.is_first_order) {
                val listOrdered = list.sortBy(_.create_time)
                listOrdered.head :: listOrdered.tail.map(info => {
                  info.is_first_order = false
                  info
                })
              }
              else it
          }
      }
    }


    // 3. 将首单的用户id 写入到hbase中
    resultStream.foreachRDD(rdd => {
      import org.apache.phoenix.spark._
      rdd
        .filter(_.is_first_order)
        .map(orderInfo => UserStatus(orderInfo.user_id.toString,true))
        .saveToPhoenix("user_status",Seq("USER_ID","IS_FIRST_ORDER"),zkUrl = Option("localhost:2181"))

       rdd.foreachPartition(
         it => {
           EsUtil.insertBulk(s"gmall_order_info_${LocalDate.now()}",it)
         }
       )

      // 4. 将数据写入到kafka的dwd中

      rdd.foreachPartition(it => {
        val producer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer()

        it.foreach(rdd => {
          implicit val f = org.json4s.DefaultFormats
          producer.send(new ProducerRecord[String,String]("dwd_order_info",Serialization.write(rdd)))
        })



        producer.close()
      })


   OffsetsManager.saveOffsets(offsetRanges,groupId,topic)
    })
  }
}
