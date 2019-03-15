package com.spark.realtime


import com.spark.realtime.utilS.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    val text: RDD[String] = sc.textFile(logFileName,200)
    // val text = sc.textFile("hdfs://10.10.4.1:8020/ibc/datalogs/apachelogs/2019/01/03/access_logs.1546444800206", 4)
    //val text = sc.textFile("data/test",1)
    //   val count=log.map((_, 1)).filter(_._1.contains("is_bc_review=1")).filter(_._1.contains("en=e_sx"))
    //        .map(_._2).sum()

    //log解析进行def
   val log= SparkUtils.LogPrase(text);
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

case  class Person(str: String, str1: String)