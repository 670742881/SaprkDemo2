package com.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App extends Object{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("logcount")

    val sc = new SparkContext(conf)


  }

}
