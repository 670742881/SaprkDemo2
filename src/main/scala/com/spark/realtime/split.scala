package com.spark.realtime

import org.apache.spark.{SparkConf, SparkContext}

object split {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("logcount")

    val sc = new SparkContext(conf)
    val text = sc.textFile("data/text")

      .map(log=>{
        var a=()
       val arr= log.split("\002")
        println(arr.length)
        (arr.length,arr(9))
      })
  text.foreach(println(_))




  }

}
