package com.spark.demo

import java.util

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes


object HbaseUtil extends  Serializable {
  val hbaseConf=HBaseConfiguration.create()
  hbaseConf.set("hbase.rootdir", "hdfs://hadoop01:9000/hbase")
  hbaseConf.set("hbase.zookeeper.quorum", "hadoop01:2181,hadoop02:2181,hadoop03:2181")

 val connection= ConnectionFactory.createConnection(hbaseConf)
  val admin=connection.getAdmin;

  //创建hbase表 String* 创建多个列簇
  def createTable(table:Table,colFamily: String*): Unit = {
    if (!admin.tableExists(TableName.valueOf("order_stastic"))) {
      val descriptor = new HTableDescriptor(TableName.valueOf("ShixunRealStatTest"))
      //foreach创建多个列簇
      colFamily.foreach(x => descriptor.addFamily(new HColumnDescriptor(x)))
      admin.createTable(descriptor)
      admin.close()
    }
  }
  //向hbase表中插入数据
  def insertHbase(rowkey: String,amountNum: Long,tableName: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName));
    val put = new Put(Bytes.toBytes(rowkey))
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("accountNum"), Bytes.toBytes(amountNum))
    table.put(put)
  }
  def scanDataFromHabse(table:Table): Unit ={
    val scan=new Scan
    scan.addFamily(Bytes.toBytes("info"))
    val scanner:ResultScanner=table.getScanner(scan)
    var result =scanner.next()
    while (result!=null){
    var cells=  result.rawCells()
      import scala.collection.JavaConversions._
      var list: util.List[Cell] =result.getColumnCells(Bytes.toBytes("info"),Bytes.toBytes("accountNum"))
      for ( i <-list ){

     println(Bytes.toString(i.getRowArray))
      }


    }
    admin.close()
  }

  def main(args: Array[String]): Unit = {
    val table = connection.getTable(TableName.valueOf("test1"));
    scanDataFromHabse(table)
  }
/*
//需求如下:
//统计出每个产品的总销量和每个产品的总订单金额
val configuration = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(configuration)
  val admin = connection.getAdmin
  val table = connection.getTable(TableName.valueOf("order_stastic"))

  val conf = new SparkConf().setAppName("tcp wc").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(3))

  //创建hbase表 String* 创建多个列簇
  def createTable(table:Table,colFamily: String*): Unit = {
    if (!admin.tableExists(TableName.valueOf("order_stastic"))) {
      val descriptor = new HTableDescriptor(TableName.valueOf("order_stastic"))
      //foreach创建多个列簇
      colFamily.foreach(x => descriptor.addFamily(new HColumnDescriptor(x)))
      admin.createTable(descriptor)
      admin.close()
    }
  }

  //向hbase表中插入数据
  def insertHbase(productId: String, accountNum: Int, amountNum: Int): Unit = {
    val put = new Put(Bytes.toBytes(productId))
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("accountNum"), Bytes.toBytes(accountNum))
    put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("amountNum"), Bytes.toBytes(amountNum))
    table put put
  }

  //查询hbase表中的数据
  def scanDataFromHabse(table:Table): Unit ={
    val scan=new Scan
    scan.addFamily(Bytes.toBytes("i"))
    val scanner:ResultScanner=table.getScanner(scan)
    var result=scanner.next()
    while (result!=null){
      println("rowkey:" + Bytes.toString(result.getRow()) + "i:accountNum,value:"
        + Bytes.toInt(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("accountNum"))));
      println("rowkey:" + Bytes.toString(result.getRow()) + ",i:amountNum,value:"
        + Bytes.toInt(result.getValue(Bytes.toBytes("i"), Bytes.toBytes("amountNum"))));
      result=scanner.next()
    }
    admin.close()
  }
  def wordCount() = {
    createTable(table,"i")
    val dstream = ssc.socketTextStream("master", 9999)
    val result = dstream.map(x => {
      val info = x.split("\\s+")
      info match {
        case Array(_, productId, accountNum, amountNum) => (productId, (accountNum.toInt, amountNum.toInt))
        case _ => null
      }
    }).filter(x => x != null) //过滤掉解析源数据失败的数据
      .reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))

    //action触发计算,并将计算结果更新到Hbase中
    result.foreachRDD(rdd => {
      rdd.foreachPartition(pa => {
        pa.foreach(p => {
          val get =new Get(Bytes.toBytes(p._1))
          val resultGet=table.get(get)
          resultGet.advance() match {
            case true=>{
              val accountNum=Bytes.toInt(resultGet.getValue(Bytes.toBytes("i"), Bytes.toBytes("accountNum")))
              val amountNum=Bytes.toInt(resultGet.getValue(Bytes.toBytes("i"), Bytes.toBytes("amountNum")))
              insertHbase(p._1,p._2._1+accountNum,p._2._2+amountNum)
            }
            case _=>insertHbase(p._1,p._2._1,p._2._2)
          }
        })
      })
    })
    scanDataFromHabse(table)

    //action触发计算,并将计算结果更新到mysql中
    //    result.foreachRDD(rdd => {
    //      rdd.foreach(
    //        x => {
    //          Class.forName("com.mysql.jdbc.Driver")
//          val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test3", "root", "123456")
//          val sql = s"select * from order_stastic where product_id=${x._1}"
//          val stmt = connection.createStatement()
//          val result = stmt.executeQuery(sql)
//          var accountNum = x._2._1
//          var amountNum = x._2._2
//          var updateSql = s"insert into order_stastic (product_id,account_num,amount_num) values(${x._1},$accountNum,$amountNum)"
//          if (result.next()) {
//            accountNum += result.getInt("account_num")
//            amountNum += result.getInt("amount_num")
//            updateSql = s"update order_stastic set account_num=$accountNum,amount_num=$amountNum where product_id=${x._1}"
//          }
//          stmt.execute(updateSql)
//        }
//      )
//    })
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
     wordCount()
    scanDataFromHabse(table)
  }
---------------------
作者：修水管的
来源：CSDN
原文：https://blog.csdn.net/xiushuiguande/article/details/79922776
版权声明：本文为博主原创文章，转载请附上博文链接！
 */


}
