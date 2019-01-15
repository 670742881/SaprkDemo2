package com.spark.sql

import org.apache.spark.sql.SparkSession

object LognUserToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Read  HiveLogUser ")
      .enableHiveSupport()    //如果要读取hive的表，就必须使用这个
      .getOrCreate()


    spark.read.table("test.uuidstat").createTempView("loginuser")
    spark.sql("select * from loginuser ").show()



  }
}
