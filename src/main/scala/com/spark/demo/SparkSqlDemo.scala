package com.spark.demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    //    val sprkConf=new SparkConf()
    //    sprkConf.setAppName(this.getClass.getSimpleName)
    //      .setMaster("local[2]")
    val sql = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val conf = new SparkConf()
      .setAppName("Spark Streaming WordCount")
      .setIfMissing("spark.master","local[4]")   //一个receiver会占用一个线程，所以当使用多个接收器的时候，注意给定的数字至少要比receiver多1个


    //优化1：调整blockInterval，根据实际处理需求
    conf.set("spark.streaming.blockInterval","500ms")
    //优化2：开启预写式日志
    conf.set("spark.streaming.receiver.writeAheadLog.enable","true")

    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc,Seconds(5))
    //开启预写式日志转换后，必须设置一个checkpoint目录来保存wal
    ssc.checkpoint("data/checkpoint")

    //1.Define the input sources by creating input DStreams 通过定义输入源来创建input DSteram


    val zkQuorum ="hadoop04:2181"
    val  topics= Map[String,Int]("topic_bc" -> 4)

    //优化3，采用多个接收器来接收数据，可以实现负载均衡，和容错
    val receive1 = KafkaUtils.createStream(ssc,zkQuorum,"re01",topics)
    val receive2 = KafkaUtils.createStream(ssc,zkQuorum,"re01",topics)
    val receive3 = KafkaUtils.createStream(ssc,zkQuorum,"re01",topics)

    val lines = receive1.union(receive2).union(receive3).map(_._2)
    val log: DStream[(String, String)] =receive1.union(receive2).union(receive3);
    print(log)


    //2.Define the streaming computations by applying transformation and output operations to DStreams. 通过调用DStreams的转化或者输出操作来定义流式计算

    //转化操作
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val pairs: DStream[(String, Int)] = words.map((_,1))
    val wc: DStream[(String, Int)] = pairs.reduceByKey(_+_)

    //输出操作
    wc.print()

    //3.Start receiving data and processing it using streamingContext.start() 启动接收器接受数据开始处理
    ssc.start()

    //4.Wait for the processing to be stopped (manually or due to any error) using streamingContext.awaitTermination(). 等待处理被停止，可能是手动或者是错误引起
    ssc.awaitTermination()

    //5.The processing can be manually stopped using streamingContext.stop(). 通过调用stop来手动停止处理
    ssc.stop()
  }




}
