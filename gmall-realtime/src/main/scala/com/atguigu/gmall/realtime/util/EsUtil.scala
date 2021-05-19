package com.atguigu.gmall.realtime.util



import com.atguigu.gmall.realtime.bean.StartupLog
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import java.time.LocalDate
import java.util.UUID
import scala.collection.AbstractIterator
import scala.collection.convert.Wrappers
import scala.collection.immutable.{StreamIterator, VectorIterator}
import scala.collection.parallel.Splitter
object EsUtil {
  val    factory=new JestClientFactory

  factory.setHttpClientConfig(
    new HttpClientConfig.Builder("http://localhost:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      // 1000毫秒  = 1s
      .connTimeout(1000*10)
      .readTimeout(1000*10)
      .build())

  def main(args: Array[String]): Unit = {
  //  insertBulk("user13",Iterator(User("sdd",1),User("dfd",3)))
  }

  def  insertBulk(indexStr:String,sources: Iterator[Object]) = {

    val client: JestClient = factory.getObject
    val bulk = new Bulk.Builder().defaultIndex(indexStr).defaultType("_doc")
/*
  id自动生成的版本。而我们需要的是 有id就传id，无id就自动生成。
    sources.map(
      source => new Index.Builder(source).build()
    ).foreach(bulk.addAction)
 */

    for (source <- sources) {
      source match {
        case (id:String,s) =>
                          val index:Index= new Index.Builder(s).id(id).build()
                          bulk.addAction(index)
        case _ =>
                          val index:Index= new Index.Builder(source).build()
                          bulk.addAction(index)
      }
    }

    client.execute(bulk.build())
    client.close()
  }

  implicit  class RichRDD(rdd:RDD[StartupLog]){

    def saveToEs(index:String) = {
      rdd.foreachPartition(it => {
        // 创立到es的连接

        val today: String = LocalDate.now.toString
        // 比如使用gmall_dau_info_2020-08-20 作为 index
        EsUtil.insertBulk(index, it)
      })


    }


  }

}
