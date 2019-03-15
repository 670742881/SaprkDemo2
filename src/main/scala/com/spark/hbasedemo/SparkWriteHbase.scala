package com.spark.hbasedemo

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @created by imp ON 2018/2/19
  */
class SparkWriteHbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableOutputFormat.OUTPUT_TABLE, "test")
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    var arrResult: Array[String] = new Array[String](1)
    arrResult(0) = "1, 3000000000";
    //arrResult(0) = "1,100,11"

    val resultRDD = sc.makeRDD(arrResult)
    val saveRDD = resultRDD.map(_.split(',')).map { arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("total"), Bytes.toBytes(arr(1)))
      (new ImmutableBytesWritable, put)
    }
    }
    println("getConfiguration")
    var c = job.getConfiguration()
    println("save")
    saveRDD.saveAsNewAPIHadoopDataset(c)

    sc.stop()
    //  spark.stop()
  }

}
