package com.spark.realtime

import java.util

import com.spark.common.LoggerUtil
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hejunhon 2018/01/23.  */
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    // 一、上下文构建
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KafkaDirectWordCount")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")//开启被压
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
    val dstream: Unit = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
      .foreachRDD(
        i => {

          val log: util.Map[String, String] = LoggerUtil.handleLog(i.toString())
          val seq = Seq {
            log
          }
          val l: Array[((String, String), (String, String))] = sc.parallelize(seq, 10)
            .map(i => (("en", i.get("en")), ("ip", i.get("ip"))))
            .take(2)
          print("sssssssssss" + l(0)._1._2)


        }

      )



    //
    //    // 三、DStream的数据处理
    //    val resultDStream = dstream
    //      .flatMap(line => line.split(" "))
    //      .filter(word => word.nonEmpty)
    //      .map(word => (word, 1))
    //      .reduceByKey(_ + _)
    //
    //    // 四、DStream结果输出
    //    // 1. 输出到driver
    //  resultDStream.print()

    // 六、启动SparkStreaming开启数据处理
    ssc.start()
    ssc.awaitTermination() // 阻塞，等待程序的遇到中断等操作

    // 七、关闭sparkstreaming的程序运行
    ssc.stop()
  }
}
