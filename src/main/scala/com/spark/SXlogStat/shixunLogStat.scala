package com.spark.SXlogStat

import com.spark.common.TimeUtil
import com.spark.realtime.utilS.SparkUtil
import org.apache.spark.{SparkConf, SparkContext}


object shixunLogStat {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("logcount")

    val sc = new SparkContext(conf)
    val text = sc.textFile("hdfs://hadoop01:8020/flume/nginxlogs/2019/01/28/access_logs.1548604802168")
    //    val set= mutable.HashSet[String]() //set放外面
    //    sc.broadcast(set)
    val logMap= SparkUtil.LogPrase(text);
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
