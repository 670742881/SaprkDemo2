import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("")
    val sc=SparkContext.getOrCreate(conf)

     val a=Seq(("a",10),(2,20),(3,30))
    val b=Seq(("a","AA"),(2,"BB"),(3,"DD"))
    val c=Seq(("a",100),(2,300),(3,2))
   val rdd1= sc.parallelize(a,1)
    val rdd2= sc.parallelize(b,1)
    val rdd3= sc.parallelize(c,1)
"innerjoin"+ (rdd1.join(rdd2).join(rdd3)).foreach(println(_))
    println("================")
    "left"+   (rdd1.leftOuterJoin(rdd2).leftOuterJoin(rdd3)).foreach(println(_))
    println("================")
    "right"+  (rdd1.rightOuterJoin(rdd2).rightOuterJoin(rdd3)).foreach(println(_))
    println("================")
    "full"+  (rdd1.fullOuterJoin(rdd2).fullOuterJoin(rdd3)).foreach(println(_))
  }
}
