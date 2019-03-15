package com.spark.hbasedemo

/**
  * @created by imp ON 2018/2/19
  */

/**
  * Spark中内置提供了两个方法可以将数据写入到Hbase：（1）、saveAsHadoopDataset；（2）、saveAsNewAPIHadoopDataset，它们的官方介绍分别如下：
  * 　　saveAsHadoopDataset： Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for that storage system. The JobConf should set an OutputFormat and any output paths required (e.g. a table name to write to) in the same way as it would be configured for a Hadoop MapReduce job.
  * 　　saveAsNewAPIHadoopDataset： Output the RDD to any Hadoop-supported storage system with new Hadoop API, using a Hadoop Configuration object for that storage system. The Conf should set an OutputFormat and any output paths required (e.g. a table name to write to) in the same way as it would be configured for a Hadoop MapReduce job.
  */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object SaprkReadHbase {

    var total:Int = 0
    def main(args: Array[String]) {
      val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("Spark Read  Hbase ")
        .enableHiveSupport()    //如果要读取hive的表，就必须使用这个
        .getOrCreate()
     val sc= spark.sparkContext
//zookeeper信息设置，存储着hbase的元信息
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum","hadoop01,hadoop02,hadoop03")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set(TableInputFormat.INPUT_TABLE, "audit_sx_20190302")

      //读取数据并转化成rdd
      val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], //定义输出key格式
        classOf[org.apache.hadoop.hbase.client.Result]) //定义输出value
      val count = hBaseRDD.count()
      println("\n\n\n:" + count)
    val logRDD= hBaseRDD.map{case (_,result) =>{
        //获取行键v
//        val rowKey = Bytes.toString(result.getRow)
//       val api_v=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("api_v")))
//        val app_id=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("app_id")))
//        val c_time=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("c_time")))
//        val ch_id=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("ch_id")))
//        val city=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("city")))
//        val province=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("province")))
//        val country=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("country")))
//        val en=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("en")))
//        val ip=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("ip")))
//        val net_t=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("net_t")))
//        val pl=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("pl")))
//        val s_time=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("s_time")))
//        val user_id=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("user_id")))
//        val uuid=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("uuid")))
//        val ver=Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("ver")))
//     EventLog(rowKey,api_v,app_id,c_time,ch_id,city,province,country,en,ip,net_t,pl,s_time,user_id,uuid,ver)
//   val list: util.List[Cell] =   result.getColumnCells(Bytes.toBytes("info"),Bytes.toBytes("c_id"))
      val a= Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("c_id")))
      }
      }
//可以转为dataframe、dataset存入hive作为宽表 或者直接进行sparkcore分析
//     val logds= logRDD.toDS()
//      logds.createTempView("event_logs")
//    val sq=  spark.sql("select * from event_logs limit 1")
//      println(sq.explain())
//      sq.show()
val a=logRDD.collect().mkString(",")
      print(a)
      sc.stop()
      spark.stop()
    }
  }

