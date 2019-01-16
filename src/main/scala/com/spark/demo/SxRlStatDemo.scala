package com.spark.demo

import java.net.URLDecoder
import java.sql.{Connection, DriverManager}

import com.spark.common.{EventLogConstants, LoggerUtil, Test, TimeUtil}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashMap

object SxRlStatDemo extends Serializable {
  val logger = Logger.getLogger(classOf[LoggerUtil])
  private val serialVersionUID = -4892194648703458595L

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("sxdemo")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
      .set("spark.streaming.backpressure.enabled", "true")
    //开启被压
    val sc = SparkContext.getOrCreate(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // 二、DStream的构建
    // kafka的Simple consumer API的连接参数， 只有两个
    // metadata.broker.list: 给定Kafka的服务器路径信息
    // auto.offset.reset：给定consumer的偏移量的值，largest表示设置为最大值，smallest表示设置为最小值(最大值&最小值指的是对应的分区中的日志数据的偏移量的值) ==> 每次启动都生效
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop04:9092,hadoop05:9092,hadoop06:9092",
      "auto.offset.reset" -> "largest",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer")
    //      "spark.serializer"->"org.apache.spark.serializer.KryoSerializer")
    // 给定一个由topic名称组成的set集合
    val topics = Set("topic_bc")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
      //      .mapog => {
      //

      //      })
      .transform(rdd => {
      rdd.map(log => {
        var map: Map[String, String] = new HashMap[String, String]
        val splits = log.split("\\^A")
        if (splits.length==3){
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
        }else{ logger.debug("次日志无法解析")}
        }
        map
      })

    })
    stream.cache()
    ssc.checkpoint("checkpoint")
    val bc_personAmt = stream.filter(log => log.contains("en") && log("en") == "e_sx")
      // combine_map.get("test_101").getOrElse("不存在") //根据key取value值,如果不存在返回后面的值
      //  scala> a.get(1)
      // res0: Option[Int] = Some(2) get返回的是Option[Int]类型 不可能等于" " ==Some("e_la")
      .map(log => (log("bc_person"), 1))
      .updateStateByKey[Long]((seq: Seq[Int], state: Option[Long]) => {
      //seq:Seq[Long] 当前批次中每个相同key的value组成的Seq
      val currentValue = seq.sum
      //state:Option[Long] 代表当前批次之前的所有批次的累计的结果，val对于wordcount而言就是先前所有批次中相同单词出现的总次数
      val preValue = state.getOrElse(0L)
      Some(currentValue + preValue)
    })

    val areaStartAmt: DStream[(String, Long)] = stream.filter(log => log.contains("en")  && log.get("en") != null)
      .map(log => (log("country") + "_" + log("provence") + "_" + TimeUtil.parseLong2String(log("s_time").toLong), 1))
      .updateStateByKey((seq: Seq[Int], state: Option[Long]) => {
        //seq:Seq[Long] 当前批次中每个相同key的value组成的Seq
        val currentValue = seq.sum
        //state:Option[Long] 代表当前批次之前的所有批次的累计的结果，val对于wordcount而言就是先前所有批次中相同单词出现的总次数
        val preValue = state.getOrElse(0L)
        Some(currentValue + preValue)
      }).transform(rdd => {
      rdd.sortBy(_._2, false)
    })
    areaStartAmt.print()


    //bc_personAmt 插入hbase

    areaStartAmt.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
//        /**
//          * *&**********************************************************************
//          *注意事项1：在各个分区内进行hbase设置，开启连接  每个分区连接一次 避免每条每条数据进行连接
//          * 注意事项2：在外部创建hbase与connect  是在diver端的代码  需要注意在foreachRDD算子进行的操作是在executor的操作 会报序列化错误
//          * 注意事项3：从中可以看出，直接把 int 型的参数传入 Bytes.toBytes() 函数中，编译不会报错，但数据的格式发生错误，再显示时就会出现乱码，
//          * 因此，在调用 Bytes.toBytes() 函数时，需要先将 int, double 型数据转换成 String 类型，此时即可正常显示。
//          *  查询会出现乱码  int double等 需要  put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("accountNum"), Bytes.toBytes(String.valueOf(record._2)))
//          *  注意事项3：使用500条一个批次提交的sql代码执行 局部更新操作 ，数据更新不知是太慢 还是未达到500条 数据库数据不正确
//          *  直接使用了 val sql1 = s"insert into area_user_amt (date,country,provence,amt)
//          *  values('${datekey}','${countrykey}','${provencekey}','${amt}') ON DUPLICATE KEY UPDATE `amt`= '${amt}'"
//          * 未使用预编译 与批次提交 实时更新  在集群模式下所以的分区与机器都访问数据库的次数过多 造成结果？？
//          *********************************************************************
//          */


        val hbaseConf = HBaseConfiguration.create()
        //        hbaseConf.set("hbase.rootdir", "hdfs://hadoop01:9000/hbase")
        //        hbaseConf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        hbaseConf.addResource("hbase-site.xml")
        val connection = ConnectionFactory.createConnection(hbaseConf)
        // val admin=connection.getAdmin;
        val table = connection.getTable(TableName.valueOf("test1"));
        if (partitionOfRecords.isEmpty) {
          println("This RDD is not null but partition is null")
        } else {
          partitionOfRecords.foreach(record => {
            val put = new Put(Bytes.toBytes(record._1))
            /*
             从中可以看出，直接把 int 型的参数传入 Bytes.toBytes() 函数中，编译不会报错，但数据的格式发生错误，再显示时就会出现乱码，
            因此，在调用 Bytes.toBytes() 函数时，需要先将 int, double 型数据转换成 String 类型，此时即可正常显示。
           ***********************************************************************
             */

            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("accountNum"), Bytes.toBytes(String.valueOf(record._2)))
            table.put(put)
          })
        }
      })
      //   HbaseUtil.scanDataFromHabse(table)
    })

    val url = "jdbc:mysql://10.20.4.222:3306/test"
    val username = "bigdata"
    val password = "bigdata"
    Class.forName("com.mysql.jdbc.Driver")
    areaStartAmt.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        if (partitionOfRecords.isEmpty) {
          println("This RDD is not null but partition is null")
        } else {

          //          Class.forName("com.mysql.jdbc.Driver")
          //          var connection: Connection = null
          //          try {
          //            connection = DriverManager.getConnection(url, username, password)
          //            connection.setAutoCommit(false)
          //            val time = System.currentTimeMillis() / 1000
          //            //  val sql = "insert into test (bc_person,amt)  values(?,?) ON DUPLICATE KEY UPDATE `amt`= ?"
          //            val sql1 = "insert into area_user_amt (date,country,provence,amt)  values(?,?,?,?) ON DUPLICATE KEY UPDATE `amt`= ?"
          //            //  val sql3 = "insert into area_user_amt (date,country,provence,amt)  values(?,?,?,?) "
          //            val pstmt = connection.prepareStatement(sql1)
          //            var count = 0
          //            partitionOfRecords.foreach(record => {
          //              //              pstmt.setString(1, record._1)
          //              //              pstmt.setInt(2, record._2.toInt)
          //              //              pstmt.setInt(3,  record._2.toInt)
          //              val info = record._1.split("_")
          //              //  if(info.length==3){
          //              pstmt.setString(1, info(2))
          //              pstmt.setString(2, info(0))
          //              pstmt.setString(3, info(1))
          //              pstmt.setInt(4, record._2.toInt)
          //              pstmt.setInt(5, record._2.toInt)
          //              pstmt.addBatch()
          //              count += 1
          //              if (count % 500 == 0) {
          //                pstmt.executeBatch()
          //                connection.commit()
          //              }
          //            })
          //            pstmt.execute()
          //            connection.commit()
          //          } finally {
          //            if (connection != null)
          //              connection.close()
          //          }

          val connection = DriverManager.getConnection(url, username, password)
          partitionOfRecords.foreach(record => {
            var datekey = record._1.split("_")(2)
            var countrykey = record._1.split("_")(0)
            var provencekey = record._1.split("_")(1)
            var amt = record._2
            val sql1 = s"insert into area_user_amt (date,country,provence,amt)  values('${datekey}','${countrykey}','${provencekey}','${amt}') ON DUPLICATE KEY UPDATE `amt`= '${amt}'"
            // val sql = s"select * from area_user_amt where date='${datekey}' and country='${countrykey}' and provence='${provencekey}'"
            val stmt = connection.createStatement()
            val code = stmt.executeUpdate(sql1)
            //返回值
            if (code < 0) {
              println("更新失败")
            }
            else {
//              println("更新成功")
         }

            //                      if (result.next()) {
            //                        val date = result.getString("date")
            //                        val country = result.getString("country")
            //                        val provence = result.getString("provence")
            //                        if (date + country + provence == datekey + countrykey + provencekey) {
            //                          val updateSql = s"update area_user_amt set amt='${amt}' where date='${datekey}' and country='${countrykey}' and provence='${provencekey}'"
            //                         val code=stmt.execute(updateSql)
            //                          println(code)
            //                        }
            //                        else {
            //                          var insertSql = s"insert into area_user_amt (date,country,provence,amt) values('${datekey}','${countrykey}','${provencekey}','${amt}')"
            //                         val cone1= stmt.execute(insertSql)
            //                          println(cone1)
            //                        }
          })
        }
      })
    })

    // areaStartAmt 插入数据
    bc_personAmt.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        if (partitionOfRecords.isEmpty) {
          println("This RDD is not null but partition is null")
        } else {

          Class.forName("com.mysql.jdbc.Driver")
          var connection: Connection = null
          try {
            connection = DriverManager.getConnection(url, username, password)
            connection.setAutoCommit(false)
            val time = System.currentTimeMillis() / 1000
            val sql = "insert into test (bc_person,amt)  values(?,?) ON DUPLICATE KEY UPDATE `amt`= ?"
            val pstmt = connection.prepareStatement(sql)
            var count = 0
            partitionOfRecords.foreach(record => {
              pstmt.setString(1, record._1)
              pstmt.setInt(2, record._2.toInt)
              pstmt.setInt(3, record._2.toInt)
              pstmt.addBatch()
              count += 1
              System.err.print(count)
              if (count % 500 == 0) {
                pstmt.executeBatch()
                connection.commit()
              }
            })
            pstmt.execute()
            connection.commit()
          } finally {
            if (connection != null)
              connection.close()
          }

        }
      })
    })
    bc_personAmt.print()
    ssc.start()
    ssc.awaitTermination() // 阻塞，等待程序的遇到中断等操作

    // 七、关闭sparkstreaming的程序运行
    ssc.stop()
  }
}

