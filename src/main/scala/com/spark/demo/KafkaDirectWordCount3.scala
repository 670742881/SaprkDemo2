//package com.spark.demo
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * Created by ibf on 10/23.
//  */
//object KafkaDirectWordCount3 {
//  def main(args: Array[String]): Unit = {
//    // 一、上下文构建
//    val conf = new SparkConf()
//      .setMaster("local[2]")
//      .setAppName("KafkaDirectWordCount3")
//      .set("spark.streaming.kafka.maxRatePerPartition", "100")
//      .set("spark.streaming.backpressure.enabled", "true")
//    val sc = SparkContext.getOrCreate(conf)
//    val ssc = new StreamingContext(sc, Seconds(1))
//
//    // 二、DStream的构建
//    // kafka的Simple consumer API的连接参数， 只有两个
//    // metadata.broker.list: 给定Kafka的服务器路径信息
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> "hadoop04:9092"
//    )
//    // 给定消费那个topic的那个分区以及从哪儿进行消费
//    val fromOffsets: Map[TopicAndPartition, Long] = Map(
//      TopicAndPartition("topic_bc", 0) -> 0 // 这里给定的offset偏移量值如果在kafka的对应分区中不存在，那么会报错
//      // 消费 TOPIC的分区id为1的分区的数据，从分区中的100offset偏移量开始进行消费
//    )
//    // 给定具体的消息处理函数
//    val messageHandler: MessageAndMetadata[String, String] => String = {
//      (msg: MessageAndMetadata[String, String]) => {
//       /* msg.offset // 当前这条数据所属的偏移量
//        msg.partition // 当前这条数据所属的分区id
//        msg.topic // 当前这条数据所属的topic名称
//        msg.key() // 当前这条数据对应的key
//        msg.message() // 当前这条数据对应的value*/
//        msg.message()
//      }
//    }
//
//    // 构建DStream
//    val dstream: DStream[String] = KafkaUtil.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc, kafkaParams, fromOffsets, messageHandler)
//
//
//    // 三、DStream的数据处理
////    val resultDStream = dstream
////      .flatMap(line => line.split(" "))
////      .filter(word => word.nonEmpty)
////      .map(word => (word, 1))
////      .reduceByKey(_ + _)
////
////    // 四、DStream结果输出
////    // 1. 输出到driver
////    resultDStream.print()
//    dstream.print()
//
//    // 六、启动SparkStreaming开启数据处理
//    ssc.start()
//    ssc.awaitTermination() // 阻塞，等待程序的遇到中断等操作
//
//    // 七、关闭sparkstreaming的程序运行
//    //ssc.stop()
//  }
//}
