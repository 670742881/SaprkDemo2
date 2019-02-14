package com.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.util.Random

/**
 * Hello world!
 *
 */
object App extends Object{
  def main(args: Array[String]): Unit = {
    val map =HashMap(1->2)
    val set =mutable.HashSet(1,2)
//    println(set.+(1))
//    println(set.add(3))
//    println(set)

    for (e <-1 to 10){
      set.add(e)
    }

println(set)
    println(map.get(2))
  }

}
