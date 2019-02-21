package com.spark.realtime.utilS

import java.net.URLDecoder

import com.spark.common.IP_parse.Test
import com.spark.common.TimeUtil
import com.spark.realtime.SxRlStatDemo.logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


import scala.collection.immutable.HashMap

/**
  * @created by imp ON 2019/2/14
  */
object SparkUtil {

  //1.日志解析方法
  def LogPrase(text:RDD[String]) ={

    val log = text.map(log => {
      var map: Map[String, String] = new HashMap[String, String]
      val splits = log.split("\\^A")
      val ip = splits(0).trim
      val nginxTime = TimeUtil.parseNginxServerTime2Long(splits(1).trim).toString;
      if (nginxTime != "-1") {
        nginxTime.toString
      }
      val requestStr = splits(2)
      val index1 = requestStr.indexOf("?")
      if (index1 > -1) { // 有请求参数的情况下，获取？后面的参数
        val requestBody: String = requestStr.substring(index1 + 1)
        var areaInfo = if (ip.nonEmpty) Test.getInfo(ip) else Array("un", "un", "un")
        val requestParames = requestBody.split("&")
        for (e <- requestParames) {
          val index2 = e.indexOf("=")
          if (index2 < 1) {
            logger.error(e + "次日志无法解析")
          } else {
            var key = "";
            var value = "";
            key = e.substring(0, index2)
            value = URLDecoder.decode(e.substring(index2 + 1).replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8")
            if ("null".equals(value)) value = "无数据" else value
            // value = URLDecoder.decode(e.substring(index2 + 1), EventLogConstants.LOG_PARAM_CHARSET)
            map.+=(key -> value)
          }
        }
        map.+=("ip" -> ip, "s_time" -> nginxTime, "country" -> areaInfo(0), "provence" -> areaInfo(1), "city" -> areaInfo(2))
      }
      map

    })
    log
  }


  /**
    * 根据app名称和是否是本地运行环境返回一个SparkConf对象
    *
    * @param appName app名称
    * @param isLocal 是否是本地运行环境，默认为false
    * @return
    */
  def generateSparkConf(
                         appName: String,
                         isLocal: Boolean = false,
                         setupSpecialConfig: SparkConf => Unit = (conf: SparkConf) => {}): SparkConf = {
    val conf = if (isLocal) {
      // 本地运行环境，设置master
      new SparkConf()
        .setAppName(appName)
        .setMaster("local[*]")
    } else {
      // 集群运行环境，不设置master，master参数由启动脚本给定
      new SparkConf()
        .setAppName(appName)
    }

    // 设置一些共同的配置项
    conf.set("spark.sql.shuffle.partitions", "10")
    // RDD进行数据cache的时候，内存最多允许存储的大小（占executor的内存比例），默认0.6
    // 如果内存不够，可能有部分数据不会进行cache(CacheManager会对cache的RDD数据进行管理操作<删除不会用的RDD缓存>)
    conf.set("spark.storage.memoryFraction", "0.6")
    // RDD进行shuffle的时候，shuffle数据写内存的阈值(占executor的内存比例），默认0.2
    conf.set("spark.shuffle.memoryFraction", "0.2")
    // 启动固定的内存分配模型，默认使用动态的内存模式
    conf.set("spark.memory.useLegacyMode", "true")
    // TODO: 如果发现GC频繁而且持续时间长，这两个参数适当调低

    // 参数修改
    // 如果修改的HADOOP配置项，必须在配置项key前一个前缀:"spark.hadoop."
    conf.set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", "616448000")

    // 给定sparkstreaming程序中，receiver模式下，block块生成的间隔时间
    conf.set("spark.streaming.blockInterval", "1s")

    // 设置应用特殊的/特定的参数
    setupSpecialConfig(conf)
    // 返回创建的SparkConf对象
    conf
  }



  /**
    * 根据给定的SparkConf创建SparkContext对象<br/>
    * 如果JVM中存在一个SparkContext对象，那么直接返回已有的数据，否则创建一个新的SparkContext对象并返回
    *
    * @param conf
    * @return
    */
  def getSparkContext(conf: SparkConf = new SparkConf()): SparkContext = SparkContext.getOrCreate(conf)}



//object SQLSessionUtil {
// 特别注意：一个Spark一个应用中，最好只用SQLContext对象
// @transient private var sparkSession: SparkSession = _
//  @transient private var sqlContext: SQLContext = _

/**
  * 根据给定的参数创建一个单例的SQLContext对象，如果#supportHive参数为true的时候，表示最终返回的SQLContext是需要集成Hive的；如果给定了具体的generateMockData的方法，那么必须调用该方法创建测试数据(默认不创建任何数据)
  */


//  def getInstance(
//                   sc: SparkContext,
//                   supportHive: Boolean = true,
//                   generateMockData: (SparkContext, SparkSession)  = (sc: SparkContext, sparkSession: SparkSession) => {}
//
//                 ): SparkSession = {
//    // 产生SQLContext对象
//    val context = if (supportHive) {
//      if (sparkSession == null) {
//        synchronized {
//          if (SparkSession == null) {
//            sparkSession = new SparkSession(sc)
//            //            generateMockData(sc, hiveContext)
//          }
//        }
//      }
//
//      // 返回创建好的HiveContext
//
//      //    } else {
//      //      if (sqlContext == null) {
//      //        synchronized {
//      //          if (sqlContext == null) {
//      //            sqlContext = new SQLContext(sc)
//      //            generateMockData(sc, sqlContext)
//      //          }
//      //        }
//      //      }
//      //
//      //      // 返回创建好的SQLContext对象
//      //      sqlContext
//      //    }
//      //
//      //    // 返回对象
//      context
//    }
//
//  }

