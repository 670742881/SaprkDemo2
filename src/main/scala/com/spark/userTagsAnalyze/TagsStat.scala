package com.spark.userTagsAnalyze

import com.spark.common.JsonProcessUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @created by imp ON 2019/2/25
  */
object TagsStat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[2]")
    val sc = SparkContext.getOrCreate(sparkConf)
    //读取用户评论数据 测试数据
    //77287793	{"reviewPics":null,"extInfoList":[{"title":"contentTags","values":["音响效果好","体验好","价格实惠"],"desc":"","defineType":0},{"title":"tagIds","values":["173","499","373"],"desc":"","defineType":0}],"expenseList":null,"reviewIndexes":[2],"scoreList":null}
    val log = sc.textFile("data/TagsTest/temptags.txt")
    //先分割开 店的ID 与评论信息 过滤掉小于2长度的
    val tag: RDD[(String, List[Any])] = log.map(line => (line.split("\t"))).filter(_.length == 2)
      .map(arr => {
        val shopId = arr(0)
        val userComment = JsonProcessUtil.extractTags(arr(1))
        //        (shopId->userComment)
        (shopId, userComment)
      }).filter(_._2.length > 0)
      //将评论的内容按,逗号分隔开来
      .map(str => (str._1, str._2.split(",")))
      //后面的数组[回头客,上菜快,环境优雅,性价比高,菜品不错] 压扁 变成 （userr，回头客），（userr，上菜快）
      .flatMapValues(e => e)
      .map(i => ((i._1, i._2), 1))
      .reduceByKey(_ + _)
      .map(e => (e._1._1, List(e._1._2, e._2)))

    /**
      * // List ::: 方法实现叠加功能
      * val three = one ::: two
      * // 此时 three的值是：List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      * println(three)
      */
    //list集合将当前的元祖加入到list里面 ，聚合操作

  }






}
