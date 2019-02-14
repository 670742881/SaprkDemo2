package com.spark.SXlogStat

import java.net.URLDecoder

import com.spark.common.{EventLogConstants, Test, TimeUtil}
import com.spark.demo.SxRlStatDemo.logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap
import scala.collection.mutable


object shixunLogStat {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("logcount")

    val sc = new SparkContext(conf)
    val text = sc.textFile("hdfs://hadoop01:8020/flume/nginxlogs/2019/01/28/access_logs.1548604802168")
    //    val set= mutable.HashSet[String]() //set放外面
    //    sc.broadcast(set)

    val logMap = text.filter(line => line.contains("en=e_sx")).map(log => {
      var map = new HashMap[String, String]
      val splits = log.split("\\^A")
      if (splits.length == 3) {
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
          map.+=("ip" -> ip, "s_time" -> nginxTime, "country" -> areaInfo(0), "provence" -> areaInfo(1), "city" -> areaInfo(2))
        } else {
          logger.debug("次日志无法解析")
        }
      }
      map
    })

    val audtorStat = logMap.
      filter(log => log.contains("bc_status") && log.contains("s_time") && log("bc_status") != "0" && log.contains("bc_person")
        && log("is_delete") == "0" && log("cr_cp_id") != "699004")
      .map(f = log => {


        if (log.get("is_bc_review") == None ||"1".equals(log.get("is_bc_review"))) { //None在此时代表java中的Null
         val key = TimeUtil.parseLong2String(log("s_time").toLong) + "_" + log("bc_person")
          (key, log("c_id"))
        }
        else ("11",1)
        //这个办法很low  我使用的set集合去重 逻辑不对
      }).distinct().map(i => (i._1, 1)).reduceByKey(_ + _)

    audtorStat.foreach(println(_))

    audtorStat.cache()

    audtorStat.saveAsTextFile("data/sx")
    println(audtorStat.map(_._2).sum())


  }

}
