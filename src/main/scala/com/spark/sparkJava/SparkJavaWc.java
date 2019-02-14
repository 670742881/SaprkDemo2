package com.spark.sparkJava;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkJavaWc {
    public static void main(String[] args) {
        //创建sparksession
        SparkSession spark = SparkSession.builder().appName("wc").master("local[2]").getOrCreate();
        JavaRDD<String> lines=spark.read().textFile("data/WC.txt").javaRDD();
      JavaRDD<String>  words=lines.flatMap(line->Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> wc=words.mapToPair(word->new Tuple2<String,Integer>(word,1)).reduceByKey((x, y)->x+y);
        List<Tuple2<String, Integer>> list=wc.collect();
        for (Tuple2<String, Integer> o:list){
            System.out.println(o._1+"="+o._2);

        }


        spark.stop();
    }
}
