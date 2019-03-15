package com.spark.realtime

import java.net.URLDecoder

import com.spark.common.EventLogConstants
import com.spark.common.IP_parse.Test
import kafka.serializer.StringDecoder
import net.minidev.json.JSONObject
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions.mutableMapAsJavaMap



/**
  * Created by hejunhon 2018/01/23.  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    // 一、上下文构建
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaDirectWordCount")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
    //开启被压
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // 二、DStream的构建
    // kafka的Simple consumer API的连接参数， 只有两个
    // metadata.broker.list: 给定Kafka的服务器路径信息
    // auto.offset.reset：给定consumer的偏移量的值，largest表示设置为最大值，smallest表示设置为最小值(最大值&最小值指的是对应的分区中的日志数据的偏移量的值) ==> 每次启动都生效
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop04:9092,hadoop05:9092,hadoop06:9092",
      "auto.offset.reset" -> "smallest"
    )
    // 给定一个由topic名称组成的set集合
    val topics = Set("topic_bc")
    // 构建DStream
    val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    val data = dstream.map(line => {
      line.split("\\?", -1)
    }).filter(_.length == 2).map(arr => {
      val headARR = arr(0).split("\\^A", -1)
      val tailarrr = arr(1).split("&", -1)
      (headARR, tailarrr)
    }).filter(tup => tup._1.length == 3 || tup._2.length >= 8)
      .map {
        case (arr1, arr2) => {
          val ip = arr1(0)
          val serviceTime = arr1(1)
          val map = scala.collection.mutable.Map[String, String]()
          for (e <- arr2) {
            val fieds = e.split("=", -1)
            val key = fieds(0)
            val value = URLDecoder.decode(fieds(1), EventLogConstants.LOG_PARAM_CHARSET)
            map.+=(key -> value)
          }
          var areaInfo = if (ip.nonEmpty) Test.getInfo(ip) else Array("un", "un", "un")
          map.+=("ip" -> ip, "s_time" -> serviceTime, "country" -> areaInfo(0), "provence" -> areaInfo(1), "city" -> areaInfo(2))
          map
        }
      }
    //    .map(data=>{
    //        val a=  data match {
    //            case v if (v.get("en")=="e_st")=>println(v+"===========启动日志的长度为"+v.size)
    //            case v if (v.get("en")=="e_sx")=>println(v+"============视讯日志的长度为"+v.size)
    //            case v if (v.get("en")=="e_dm")=>println(v+"============动漫日志的长度为"+v.size)
    //            case v if (v.get("en")=="e_pv")=>println(v+"============浏览日志的长度为"+v.size)
    //          }
    //        }).print()
    //  val a = data.map(map=>{
    //     map.get("en") match {
    //       case v if (v=="e_st")=>Some("===========启动日志的长度为"+v.size)
    //       case v if (v=="e_sx")=>Some("============视讯日志的长度为"+v.size)
    //        case v if (v=="e_dm")=>Some("============动漫日志的长度为"+v.size)
    //        case v if (v=="e_pv")=>Some("============浏览日志的长度为"+v.size)
    //       case _ => None
    //     }
    //   }
    //   ).map(_.print()

    data.map(map => {
      if (map.get("en") == Some("e_st")) map.size + "启动日志的长度为"
      else if (map.get("en") == Some("e_sx")) map.size + "视讯日志的长度为"
      else if (map.get("en") == Some("e_dm")) map.size + "动漫日志的长度为"
      else if (map.get("en") == Some("e_pv")) map.size + "浏览日志的长度为"
      else if (map.get("en") == Some("e_la")) map.size + "新增事件日志的长度为"
      else if (map.get("en") == Some("live")) map.size + "直播日志的长度为"
      else map.size + map.get("en").toString
    }
    )


//    data.saveAsTextFiles("data/logs/res")
  val a =  data.map(map=>JSONObject.toJSONString(map)).print()

    /**
      * 由于日志的格式有多个  我将它转为json 可以做后续处理 存入hive 或者 hbase  fastjon用不了
      *
      *
      */
    data.map(_.toList.mkString(",")).print()





    // 六、启动SparkStreaming开启数据处理
    ssc.start()
    ssc.awaitTermination() // 阻塞，等待程序的遇到中断等操作

    // 七、关闭sparkstreaming的程序运行
    ssc.stop()
  }
}
