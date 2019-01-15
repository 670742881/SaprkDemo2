package com.spark.demo

import java.net.URLDecoder

import com.spark.common.{Test, TimeUtil}
import com.spark.demo.SxRlStatDemo.logger
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap

object UserIdAnalyze {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession
      .builder()
      .appName("AccesslogDataFrame")
//      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    //创建sparkContext
    val sc=spark.sparkContext
  val files=HdfsUtil.getAllFiles("hdfs://10.10.4.1:8020/ibc/datalogs/apachelogs/archive/2018")
  for (file<-files) {
    val logFileName: String = file.getPath.getName
    val   logpath=file.getPath

//    println(file.getPath.toString)
//    println(logFileName)
    val text = sc.textFile(logFileName,200)
    // val text = sc.textFile("hdfs://10.10.4.1:8020/ibc/datalogs/apachelogs/2019/01/03/access_logs.1546444800206", 4)
    //val text = sc.textFile("data/test",1)
    //   val count=log.map((_, 1)).filter(_._1.contains("is_bc_review=1")).filter(_._1.contains("en=e_sx"))
    //        .map(_._2).sum()
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
    val uuid= log.filter(log=>log.contains("userId")).map(log=>logFileName.substring(0,6)+"\t"+log("userId")).distinct().cache()
//    uudi.repartition(1).saveAsTextFile("1")
    import spark.implicits._

    val userDF=uuid.map(x=>{
     val date=x.split("\t")(0)
      val userid=x.split("\t")(1)
   Person(date,userid)
    }).toDF()
    userDF.show()
userDF.createTempView("userstat")
    spark.read.table("test.uuidstat").createTempView("ss")
    val user=spark.sql("insert into table ss select * from userstat")
  }

   HdfsUtil.close()
  }
}
