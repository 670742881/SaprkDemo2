package com.spark.userTagsAnalyze





import com.spark.common.JsonProcessUtil
import org.apache.spark.{SparkConf, SparkContext}
object TagGenerator {
    def main(args: Array[String]) = {
        val conf = new SparkConf();
        conf.setAppName("tagGen");
        conf.setMaster("local[4]");
        val sc = new SparkContext(conf)
//        val sqlContext = new HiveContext(sc)
//        import sqlContext.implicits._

        val poi_tags = sc.textFile("data/TagsTest/temptags.txt").coalesce(1,false)
        val poi_taglist = poi_tags.map(e => e.split("\t"))
            .filter(e => e.length == 2).
            //77287793 -> 音响效果好,干净卫生,服务热情
            map(e => e(0) -> JsonProcessUtil.extractTags(e(1))).
            //过滤评论串不为0
            filter(e => e._2.length > 0)
            //77287793 -> [音响效果好,干净卫生,服务热情]
            .map(e => e._1 -> e._2.split(","))
            //77287793 -> 音响效果好 , 77287793->干净卫生,77287793->服务热情
            .flatMapValues(e => e)
            //(77287793,音响效果好)->1,(77287793,干净卫生)->1,(77287793,服务热情)->1
            .map(e => (e._1, e._2) -> 1)
            //聚合(77287793,音响效果好)->340
            .reduceByKey(_ + _)
            //77287793->List(音响效果好,340)
            .map(e => e._1._1 -> List((e._1._2, e._2)))
          //77287793->List((音响效果好,340),(干净卫生,400),(..))
          .reduceByKey(_ ::: _)
          //77287793->List((音响效果好,540),(干净卫生,400),(..))===>
          //77287793->List((音响效果好,540),(干净卫生,400),(..),..)===>
          //77287793->List(音响效果好:540,干净卫生:400),(..),..)===>
          //77287793->音响效果好:540,干净卫生:400
          .map(e => e._1 -> e._2.sortBy(_._2).reverse.take(10).map(a => a._1 + ":" + a._2.toString).mkString(","))

        poi_taglist.map(e => e._1 + "\t" + e._2).saveAsTextFile("data/TagsTest/res");
    }
}
