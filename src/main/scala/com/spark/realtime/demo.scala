package com.spark.realtime



  import java.net.URLDecoder

  import com.spark.common.IP_parse.Test
  import com.spark.common.{EventLogConstants, LoggerUtil, TimeUtil}
  import com.spark.realtime.utilS.SparkUtil
  import kafka.serializer.StringDecoder
  import org.apache.log4j.Logger
  import org.apache.spark.streaming.dstream.DStream
  import org.apache.spark.streaming.kafka.KafkaUtils
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.{SparkConf, SparkContext}

  import scala.collection.immutable.HashMap

  object demo extends Serializable {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      val logger = Logger.getLogger(classOf[LoggerUtil])
      conf.setMaster("local[2]").setAppName("sxdemo")
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
        "auto.offset.reset" -> "largest"
      )
      // 给定一个由topic名称组成的set集合
      val topics = Set("topic_bc")
      val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
        //      .mapog => {
        //

        //      })
        .transform(rdd => {//        SparkUtil.LogPrase(rdd)

        rdd.map(log => {
          var map: Map[String, String] = new HashMap[String, String]
          val splits = log.split("\\^A")
          val ip = splits(0).trim
          val nginxTime = TimeUtil.parseNginxServerTime2Long(splits(1).trim).toString;
          if (nginxTime != "-1") {
            nginxTime.toString
          }
          val requestStr = splits(2)
          val index = requestStr.indexOf("?")
          if (index > -1) { // 有请求参数的情况下，获取？后面的参数
            val requestBody: String = requestStr.substring(index + 1)
            var areaInfo = if (ip.nonEmpty) Test.getInfo(ip) else Array("un", "un", "un")
            val requestParames = requestBody.split("&")
            for (e <- requestParames) {
              val index = e.indexOf("=")
              if (index < 1) {
                logger.debug("次日志无法解析")
              }
              var key = ""; var value = "";
              key = e.substring(0, index)
              value = URLDecoder.decode(e.substring(index + 1), EventLogConstants.LOG_PARAM_CHARSET)
              map.+=(key -> value)
            }
//            //有的日志新增了url字段
//            if (map("url")!=null) {
//              val index = map("url").indexOf("?")
//              if (index > -1) { // 有请求参数的情况下，获取？后面的参数
//                val requestBody: String = requestStr.substring(index + 1)
//                val requestParames = map("url").split("&")
//                for (e <- requestParames) {
//                  val index = e.indexOf("=")
//                  if (index < 1) {
//                    logger.debug("次日志无法解析")
//                  }
//                  var key = ""; var value = "";
//                  key = e.substring(0, index)
//                  value = URLDecoder.decode(e.substring(index + 1), EventLogConstants.LOG_PARAM_CHARSET)
//                  map.+=(key -> value)
//                }
//              }
//            }
            map.+=("ip" -> ip, "s_time" -> nginxTime, "country" -> areaInfo(0), "provence" -> areaInfo(1), "city" -> areaInfo(2))

          }
          map
        })

      })
      stream.print()
      ssc.start()
      ssc.awaitTermination() // 阻塞，等待程序的遇到中断等操作

      // 七、关闭sparkstreaming的程序运行
      ssc.stop()
    }


}
