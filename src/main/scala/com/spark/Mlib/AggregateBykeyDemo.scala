package com.spark.Mlib

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @created by imp ON 2019/2/18
  */
object AggregateBykeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("MLDEMO")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = Array(1, 2,3, 4, 5, 6, 7, 8, 9)
    val distData = sc.parallelize(data, 3)

//1.text
    val distFile1 = sc.textFile("data.txt") //本地当前目录下文件
    val distFile2 =sc.textFile("hdfs://192.168.1.100:8020/input/data.txt") //HDFS文件
    val distFile3 =sc.textFile("file:/input/data.txt") //本地指定目录下文件
    val distFile4 =sc.textFile("/input/data.txt") //本地指定目录下文件
//    注意：textFile可以读取多个文件，或者1个文件夹，也支持压缩文件、包含通配符的路径。
//    textFile("/input/001.txt, /input/002.txt ") //读取多个文件
//    textFile("/input") //读取目录
//    textFile("/input /*.txt") //含通配符的路径
//    textFile("/input /*.gz") //读取压缩文件

    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.map(x => x*2)
    rdd2.collect
//    res3: Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18)
    /**
      *
      * val data = Array(1, 2,3, 4, 5, 6, 7, 8, 9)
      * val distData = sc.parallelize(data, 3)
      * aggreateByKey(zeroValue: U)(seqOp: (U, T)=> U, combOp: (U, U) =>U) 和reduceByKey的不同在于，
      * reduceByKey输入输出都是(K, V)，而aggreateByKey输出是(K,U)，可以不同于输入(K, V) ，
      * aggreateByKey的三个参数： zeroValue: U，初始值，比如空列表{} ；
      * seqOp: (U,T)=> U，seq操作符，描述如何将T合并入U，比如如何将item合并到列表 ；
      * combOp: (U,U) =>U，comb操作符，描述如果合并两个U，比如合并两个列表 ；
      * 所以aggreateByKey可以看成更高抽象的，更灵活的reduce或group 。
      */
    val rdd: RDD[(Int, Int)] = sc.parallelize(Seq((1, 3), (2, 4), (6, 8), (11, 100)))
    val list: Seq[Int] = List(1, 2, 3, 6)
    val rdd11 = sc.parallelize(list, 2)
    //1.aggregateByKey
    val r1 = rdd11.aggregate(0)(math.max(_, _), _ + _)
    println(r1)
    val r2 = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    r2.foreach(println(_))


    /**
      *
      * 16）cogroup cogroup(otherDataset, [numTasks])是将输入数据集(K, V)和另外一个数据集(K, W)进行cogroup，得到一个格式为(K, Seq[V], Seq[W]) 的数据集。
      * val rdd16 = rdd0.cogroup(rdd0) rdd16.collect res38: Array[(Int, (Iterable[Int], Iterable[Int]))] = Array((1,(ArrayBuffer(1, 2, 3),ArrayBuffer(1, 2, 3))), (2,(ArrayBuffer(1, 2, 3),ArrayBuffer(1, 2, 3))))
      */
    val rdd3 = sc.parallelize(Seq((1,(3,2,4))))
    val coGroup: RDD[(Int, (Iterable[(Int, Int, Int)], Iterable[(Int, Int, Int)]))] = rdd3.cogroup(rdd3)
    coGroup.map{
      case(key,(iter1,iter2))=>{
        (key,iter1.toList,iter2.toList)
      }
    }
    println(coGroup.collect().toList.toString())

    //sample
    val a = sc.parallelize(1 to 10000, 3)
    a.sample(false, 0.1, 0).count

  }
}




