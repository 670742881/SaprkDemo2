package com.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameWordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()
    import spark.implicits._
    val linesDF = spark.sparkContext.textFile("data/*.txt").toDF("line")
    linesDF.show(false)
    linesDF.printSchema()
    //将一行数据展开
    val wordsDF: DataFrame = linesDF.explode("line", "word")((line: String) => line.split(" "))
    wordsDF.printSchema()
    wordsDF.show(200,false)
    //对 "word"列进行聚合逻辑并使用count算子计算每个分组元素的个数
    val wordCoungDF = wordsDF.groupBy("word").count()
    wordCoungDF.show(false)
    wordCoungDF.printSchema()
    println(wordCoungDF.count() + "----------")
  }
}
