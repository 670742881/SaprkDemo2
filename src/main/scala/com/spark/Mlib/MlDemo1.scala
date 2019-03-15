package com.spark.Mlib

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.SparkSession


/**
  * @created by imp ON 2019/2/26
  */
object MlDemo1 {
  def main(args: Array[String]): Unit = {
    //数据目录
    //        val dataDir = "file:///D:/downloads/bigdata/ml/winequality-white.csv"
    //        //定义样例类
    //        case class Wine(FixedAcidity: Double, VolatileAcidity: Double,
    //                        CitricAcid: Double, ResidualSugar: Double, Chlorides: Double,
    //                        FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH:
    //                        Double, Sulphates: Double, Alcohol: Double, Quality: Double)
    //
    //        //变换
    //        val wineDataRDD = sc.textFile(dataDir).map(_.split(";")).map(w => Wine(w(0).toDouble, w(1).toDouble,
    //            w(2).toDouble, w(3).toDouble, w(4).toDouble, w(5).toDouble, w(6).toDouble, w(7).toDouble, w(8).toDouble
    //            , w(9).toDouble, w(10).toDouble, w(11).toDouble))
    //

    val sess = SparkSession.builder()
      .master("local[4]")
      .appName("Ml")
      .getOrCreate()
    //
    //        //转换RDD成DataFrame
    //        val trainingDF = wineDataRDD.map(w => (w.Quality,
    //            Vectors.dense(w.FixedAcidity, w.VolatileAcidity, w.CitricAcid,
    //                w.ResidualSugar, w.Chlorides, w.FreeSulfurDioxide, w.TotalSulfurDioxide,
    //                w.Density, w.PH, w.Sulphates, w.Alcohol))).toDF("label", "features")
    //        //显式数据
    //        trainingDF.show()
    //        println("======================")
    //
    //        //创建线性回归对象
    //        val lr = new LinearRegression()
    //        //设置最大迭代次数
    //        lr.setMaxIter(50)
    //通过线性回归拟合训练数据，生成模型
    //        val model = lr.fit(trainingDF)
    //
    //        model.save("file:///d:/scala/model");
    val model = LinearRegressionModel.load("file:///d:/scala/model");

    //创建内存测试数据数据框
    val testDF = sess.createDataFrame(Seq((6.0, Vectors.dense(7, 0.27, 0.36, 20.7, 0.045, 45, 170, 1.001, 3, 0.45, 8.8)),
      (6.0, Vectors.dense(6.3, 0.3, 0.34, 1.6, 0.049, 14, 132, 0.994, 3.3, 0.49, 9.5)),
      (6.0, Vectors.dense(8.1, 0.28, 0.4, 6.9, 0.05, 30, 97, 0.9951, 3.26, 0.44, 10.1)))).toDF("label", "features")

    testDF.show()

    //创建临时视图
    testDF.createOrReplaceTempView("test")
    println("======================")
    //利用model对测试数据进行变化，得到新数据框，查询features", "label", "prediction方面值。
    val tested = model.transform(testDF).select("features", "label", "prediction");
    tested.show();
    //
    println("======================")
    val featureDF = sess.sql("SELECT features FROM test");

    //对特征数据进行模型变换，得到预测结果
    val predictedDF = model.transform(featureDF).select("features", "prediction")
    predictedDF.show()
  }
}

