import java.net.URLDecoder

import com.spark.common.{Test, TimeUtil}
import com.spark.demo.HdfsUtil
import com.spark.demo.SxRlStatDemo.logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object LogCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("logcount")

    val sc = new SparkContext(conf)
    val files=HdfsUtil.getAllFiles("hdfs://10.10.4.1:8020/ibc/datalogs/apachelogs/archive/2018")
    for (file<-files){

    val logFileName=  file.getPath.getName
      println(logFileName)
    }

    val text = sc.textFile("data/test")

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
            logger.error(e+"次日志无法解析")
          }else{
          var key = ""; var value = "";
          key = e.substring(0, index2)
          value = URLDecoder.decode(e.substring(index2 + 1).replaceAll("%(?![0-9a-fA-F]{2})", "%25"), "UTF-8")
            if ("null".equals(value)) value="无数据" else value
         // value = URLDecoder.decode(e.substring(index2 + 1), EventLogConstants.LOG_PARAM_CHARSET)
          map.+=(key -> value)
        }
        }
        map.+=("ip" -> ip, "s_time" -> nginxTime, "country" -> areaInfo(0), "provence" -> areaInfo(1), "city" -> areaInfo(2))
      }
      map

    }).cache()
    val contInfo = log.filter(i => i.contains("contId") && i.contains("contName") && i.contains("srcContType") && i.contains("srcContId")&&i.contains("picMaker"))
      .map(i => {
//
//        /**
//          * val appId: String = paramMap.get("appid")
//          * val platform: String = paramMap.get("clienttype")
//          * val clientChannel: String = paramMap.get("clientchannel")
//          * val clientVersion: String = paramMap.get("clientversion")
//          */
        val key: String = i.getOrElse("appId", " ") + "_" + i.getOrElse("clientType", " ") + "_" + i.getOrElse("clientChannel", " ") + "_" + i.getOrElse("clientVersion", " ")
        i.+("md" -> key)
      }).map(i => {


      val key = i.getOrElse("md", " ") + "_" + TimeUtil.parseLong2String(i.getOrElse("s_time", " ").toLong) + "_" + i.getOrElse("menuId", " ") + "_" + i.getOrElse("menuName", " ") + "_" + i.getOrElse("areaId", " ") + "_" + i.getOrElse("areaName", " ") + "_" + i.getOrElse("contId", " ") + "_" + i.getOrElse("contName", " ") + "_" + i.getOrElse("srcContId", " ") + "_" + i.getOrElse("srcContType", " ") + "_" + i.getOrElse("srcContName", " ")
      val value = (i.getOrElse("ip", " "), i.getOrElse("uuid", " "), i.getOrElse("url", " "))
      (key, value)
    })

    val ipStat = contInfo.map((
      i => {
        (i._1 + "_" + i._2._1, 1)
      }
      )).distinct().reduceByKey(_ + _)
    val pvStat = contInfo.map((
      i => {
        (i._1 + "_" + i._2._3, 1)
      }
      )).reduceByKey(_ + _)
    val uvStat = contInfo.map((
      i => {
        (i._1 + "_" + i._2._2, 1)
      }
      )).distinct().reduceByKey(_ + _)
////  val joinData: RDD[(String, ((Int, Option[Int]), Option[Int]))] =pvStat.leftOuterJoin(ipStat).leftOuterJoin(uvStat);
////    // log.saveAsTextFile("data/home")
//        )
//      }
   val uv= log.filter(_.contains("uuid")).map(i=>(i("uuid"),1)).distinct().reduceByKey(_+_)
    uv.foreach(println(_))
//      println("ip===============")
//    ipStat.foreach(println(_))
//    println("pv===============")
//    pvStat.foreach(println(_))
//    println("uv===============")
//    uvStat.foreach(println(_))



}
}