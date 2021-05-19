package com.atguigu.gmall.realtime.dwd

import com.atguigu.gmall.realtime.BaseAppV2
import com.atguigu.gmall.realtime.bean._
import com.atguigu.gmall.realtime.util.OffsetsManager
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.json4s.Formats
import org.json4s.jackson.JsonMethods

import java.util.Properties
import scala.collection.mutable.ListBuffer

/**
 * 将kafka维度表的数据，写入到hbase中
 * 用一个流，消费ods层所有的维度表（每个流对应一个纬度表，代价太高）。
 * 根据不同的数据，将数据写入到hbase不同的表里面
 */
/* rdd.cache()
   rdd.collect().foreach(println)
   */

// saveToPhoenix 写了2遍，

object DwdDimAppV2 extends BaseAppV2{
  override var appName: String = "DwdDimApp"
  override var groupId: String = "DwdDimApp"
  override var topics: Seq[String] = Seq(
  "ods_user_info",
  "ods_sku_info",
  "ods_spu_info",
  "ods_base_category3",
  "ods_base_province",
  "ods_base_trademark")

  def saveToPhoenix[T<:Product](rdd: RDD[(String, String)], ods_tablename: String, table_Phoenix: String, colums_Phoenix: Seq[String])(implicit formats: Formats, mf: scala.reflect.Manifest[T]) = {
    rdd.filter(_._1 == ods_tablename).map{
      case(topic,content) => {
     //    implicit val f = org.json4s.DefaultFormats
        // extract[T]需要传 隐式值，所以函数的参数加了一堆。 上边一行也可以注释了
        JsonMethods.parse(content).extract[T](formats,mf)
      }
// 这个T一定是需要是 样历类  . 需要加个  T<:Product
    }.saveToPhoenix(table_Phoenix,
      colums_Phoenix,
      zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))
  }

  override def run(ssc: StreamingContext, offsetRanges: ListBuffer[OffsetRange], sourceStream: DStream[(String,String)]): Unit = {
    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._
    implicit val f = org.json4s.DefaultFormats
    sourceStream.foreachRDD(rdd => {
      // 需要将 rdd里的string 转化成样例类 . 不同的topic 写入到不同的表中  ，RDD[String] 只有具体的数据，没有topic信息
      topics.foreach{
        // 往hbase里写数据
        case "ods_base_province" =>   // 省份直接抄userInfo的就行，懒得写了 。
        case  "ods_user_info" =>
          saveToPhoenix[UserInfo](rdd,"ods_user_info","gmall_user_info",Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "AGE_GROUP", "GENDER_NAME"))
        case "ods_spu_info" =>
          saveToPhoenix[SpuInfo](rdd,
            "ods_spu_info",
            "gmall_spu_info",
            Seq("ID", "SPU_NAME"))
        case "ods_base_category3" =>
          saveToPhoenix[BaseCategory3](rdd,
            "ods_base_category3",
            "gmall_base_category3",
            Seq("ID", "NAME", "CATEGORY2_ID"))
        case "ods_base_trademark" =>
          saveToPhoenix[BaseTrademark](rdd,
            "ods_base_trademark",
            "gmall_base_trademark",
            Seq("ID", "TM_NAME"))

          // sku 的数据，用到了其他3张维度表 BaseTrademark，SpuInfo，BaseCategory3

        case "ods_sku_info" =>

   /*       saveToPhoenix[SkuInfo](rdd,
            "ods_sku_info",
            "gmall_sku_info",
            Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"))*/
          // 需要和 gmall_spu_info gmall_base_category3  gmall_base_trademark 连接, 然后得到所有字段
          // 使用 spark-sql 完成
          import org.apache.phoenix.spark._
          val url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
      //    spark.read.jdbc(url, "gmall_sku_info", new Properties()).createOrReplaceTempView("sku")
          spark.read.jdbc(url, "gmall_spu_info", new Properties()).createOrReplaceTempView("spu")
          spark.read.jdbc(url, "gmall_base_category3", new Properties()).createOrReplaceTempView("category3")
          spark.read.jdbc(url, "gmall_base_trademark", new Properties()).createOrReplaceTempView("tm")
         rdd
           .filter(_._1 == "ods_sku_info")
           .map{
           case (_,content) =>
             JsonMethods.parse(content).extract[SkuInfo]
         }
           .toDS()
           .createOrReplaceTempView("sku")

          spark.sql(
            """
              |select
              |    sku.id as id,
              |    sku.spu_id spu_id,
              |    sku.price price,
              |    sku.sku_name sku_name,
              |    sku.tm_id  tm_id,
              |    sku.category3_id  category3_id,
              |    sku.create_time  create_time,
              |    category3.name  category3_name,
              |    spu.spu_name  spu_name,
              |    tm.tm_name  tm_name
              |from sku
              |join spu on sku.spu_id=spu.id
              |join category3 on sku.category3_id=category3.id
              |join tm on sku.tm_id=tm.id
              |""".stripMargin)
            // .show(10)
            .as[SkuInfo]
            .rdd
            .saveToPhoenix(
              "gmall_sku_info",
              Seq("ID", "SPU_ID", "PRICE", "SKU_NAME", "TM_ID", "CATEGORY3_ID", "CREATE_TIME", "CATEGORY3_NAME", "SPU_NAME", "TM_NAME"),
              zkUrl = Option("hadoop102,hadoop103,hadoop104:2181"))

      }
    })
      OffsetsManager.saveOffsets(offsetRanges,groupId,topics)
  }
}
