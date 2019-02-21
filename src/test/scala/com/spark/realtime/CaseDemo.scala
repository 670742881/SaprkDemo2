package com.spark.realtime

import com.spark.realtime.utilS.SparkUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @created by imp ON 2019/2/14
  */
case class Person1(name:String,age:Int)
object CaseDemo {
  def main(args: Array[String]): Unit = {
    val appName = this.getClass.getName
    val isLocal = true
    val conf = SparkUtil.generateSparkConf(appName, isLocal, that => {
      // 这里可以单独的指定当前spark应用的相关参数
      // nothings
      that.set("", "")
    })
    // 2.3 SparkContext对象的构建
    val sc = SparkUtil.getSparkContext(conf)
    //可单独写方法判断 读取hdfs的某一天文件夹下所以文件
    val path = "data/person"
    val data: RDD[Any] = sc.textFile(path).map {
      //case替代map(line=>)的写法 不在使用._1 ._2
      case (line) => {
        val arr = line.split(" ")
        (arr(0), arr(1))
      }
    }
      .map(info => {
        Person1(info._1, info._2.toInt)
      })
      .map {
        //case（p）相当于 map（p=>{}）
        case (person) => {
          (person, (person.name, person.age))
        }

      }.map {
      case (person, (name, age)) => {

        val map = mutable.HashMap[String, Int]()
        map.+=(name -> age)
      }

    }
    data.foreach(println(_))


  }

}
