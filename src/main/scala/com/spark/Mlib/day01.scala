package com.spark.Mlib

/**
  * @created by imp ON 2019/2/18
  */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object day01 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("rdd_test01")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // 2.1节
    // 2.1.1 RDD 创建操作
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val distData = sc.parallelize(data, 3)

    val distFile1 = sc.textFile("data.txt") // 本地当前目录下的文件
    val distFile2 = sc.textFile("hdfs://192.168.1.100:8020/input/data.txt") // HDFS 文件
    val distFile3 = sc.textFile("file:/input/data.txt") // 本地指定目录下的文件
    val distFile4 = sc.textFile("/input/data.txt") // 本地指定目录下的文件

    // 2.1.2 RDD 转换操作
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.map(x => x * 2)
    rdd2.collect

    val rdd3 = rdd2.filter(x => x > 10)
    rdd3.collect
    val rdd4 = rdd3.flatMap(x => x to 20)

    val rdd5 = rdd1.mapPartitions(myfunc)
    rdd5.collect

    val c = sc.parallelize(1 to 10000, 3)
    c.sample(false, 0.1, 0).count

    val rdd8 = rdd1.union(rdd3)
    rdd8.collect

    val rdd9 = rdd8.intersection(rdd1)
    rdd9.collect

    val rdd10 = rdd8.union(rdd9).distinct
    rdd10.collect

    val rdd0 = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)), 3)
    val rdd11 = rdd0.groupByKey()
    rdd11.collect

    val rdd12 = rdd0.reduceByKey((x, y) => x + y)
    rdd12.collect

    // val z = sc.parallelize(List(1, 2, 3, 4, 5, 6), 2)
    //z.aggregate(0)(math.max(_, _),  _ + _)
    val z = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)))
    z.aggregateByKey(0)(math.max(_, _), _ + _).collect

    val data2 = Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0))
    val rdd = sc.parallelize(data2, 2)
    val combine1 = rdd.combineByKey(createCombiner = (v: Double) => (v: Double, 1),
      mergeValue = (c: (Double, Int), v: Double) => (c._1 + v, c._2 + 1),
      mergeCombiners = (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
      numPartitions = 2)
    combine1.collect

    val rdd14 = rdd0.sortByKey()
    rdd14.collect

    val rdd15 = rdd0.join(rdd0)
    rdd15.collect

    val rdd16 = rdd0.cogroup(rdd0)
    rdd16.collect

    val rdd17 = rdd1.cartesian(rdd3)
    rdd17.collect

    val rdd18 = sc.parallelize(1 to 9, 3)
    val a: Array[String] = rdd18.pipe("head -n 1").collect

    val rdd19: Array[RDD[Int]] = rdd1.randomSplit(Array(0.3, 0.7), 1) //对rdd随机划分函数
    rdd19(0).collect
    rdd19(1).collect
    /**
      * scala> rdd19(0).count
      * res15: Long = 7
      *
      * scala> rdd19(1).count
      * res16: Long = 2
      */

    val rdd20_1 = sc.parallelize(1 to 9, 3)
    val rdd20_2 = sc.parallelize(1 to 3, 3)
    val rdd20_3 = rdd20_1.subtract(rdd20_2) //rdd1不在rdd2中的元素

    val rdd21_1 = sc.parallelize(Array(1, 2, 3, 4), 3)
    val rdd21_2 = sc.parallelize(Array("a", "b", "c", "d"), 3)
    val rdd21_3 = rdd21_1.zip(rdd21_2) //rdd1与rdd2按列合并

    val data3 = Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0))
    val rdd24 = sc.parallelize(data3, 2)
    val combine24_1 = rdd24.combineByKey(createCombiner = (v: Double) => (v, 1), //相同key的value输入值
      mergeValue = (c: (Double, Int), v: Double) => (c._1 + v, c._2 + 1), //进行相同的key求和与次数统计
      mergeCombiners = (c1: (Double, Int), c2: (Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2), //对上一步的结果，进行所有key相加,所有value相加
      numPartitions = 2)
    //scala> combine24_1.collect
    //res19: Array[(Int, (Double, Int))] = Array((2,(15.0,3)), (1,(6.0,3)))


    val treeAggregate24_1 = rdd24.treeAggregate((0, 0.0))(seqOp = ((u, t) => (u._1 + t._1, u._2 + t._2)), //所有的u，t  u代表上次的求和结果数据，t代表当前值
      combOp = (u1, u2) => (u1._1 + u2._1, u1._2 + u2._2),
      depth = 2)

    // 2.1.3 RDD 行动操作
    val rdd3_1 = sc.parallelize(1 to 9, 3)
    val rdd3_2 = rdd3_1.reduce(_ + _)
    rdd3_1.collect()
    rdd3_1.count()
    rdd3_1.first()
    rdd3_1.take(3)
    rdd3_1.takeSample(true, 4)
    rdd3_1.takeOrdered(4)
  }

  def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next
      res.::=(pre, cur)
      pre = cur
    }
    res.iterator
  }
}
// /* * Arraylist底层的新增元素原理
//    * public void add(int index, E element) {
//    * //先判断是否超出范围，报数组下标越界异常
//    * rangeCheckForAdd(index);
//    *
//    * //确实是否扩容 在默认的10个长度与目前的size+1比较
//    * ensureCapacityInternal(size + 1);  // Increments modCount!!
//    * //将老的copy到新长度的数组
//    *         System.arraycopy(elementData, index, elementData, index + 1,
//      * size - index);
//    * elementData[index] = element;
//    * size++;
//    * }
//  *
//  *
//  *  2）外部数据源
//  * val distFile1 = sc.textFile("data.txt") //本地当前目录下文件
//  * val distFile2 =sc.textFile("hdfs://192.168.1.100:9000/input/data.txt") //HDFS文件
//  * val distFile3 =sc.textFile("file:/input/data.txt") //本地指定目录下文件
//  * val distFile4 =sc.textFile("/input/data.txt") //本地指定目录下文件 注意：textFile可以读取多个文件，或者1个文件夹，也支持压缩文件、包含通配符的路径。
//  * textFile("/input/001.txt, /input/002.txt ")  //读取多个文件 textFile("/input")
//  * //读取目录 textFile("/input /*.txt")
//  * //含通配符的路径 textFile("/input /*.gz")  //读取压缩文件
//  *
//  * 3）map
//  * map是对RDD中的每个元素都执行一个指定的函数来产生一个新的RDD；RDD之间的元素是一对一关系；
//  * val rdd1 = sc.parallelize(1 to 9, 3)
//  * val rdd2 = rdd1.map(x => x*2)
//  * rdd2.collect
//  * res3: Array[Int] = Array(2, 4, 6, 8, 10, 12, 14, 16, 18)
//  *
//  * 4）filter
//  * Filter是对RDD元素进行过滤；返回一个新的数据集，是经过func函数后返回值为true的原元素组成；
//  * val rdd3 = rdd2. filter (x => x> 10)
//  * rdd3.collect
//  * res4: Array[Int] = Array(12, 14, 16, 18)
//  *
//  * 5）flatMap
//  * flatMap类似于map，但是每一个输入元素，会被映射为0到多个输出元素（因此，func函数的返回值是一个Seq，而不是单一元素），
//  * RDD之间的元素是一对多关系；
//  * val rdd4 = rdd3. flatMap (x => x to 20)
//  * res5: Array[Int] = Array(12, 13, 14, 15, 16, 17, 18, 19, 20, 14, 15, 16, 17, 18, 19, 20, 16, 17, 18, 19, 20, 18, 19, 20)
//  *
//  * 6）mapPartitions
//  * mapPartitions是map的一个变种。map的输入函数是应用于RDD中每个元素，而mapPartitions的输入函数是每个分区的数据，也就是
//  * 把每个分区中的内容作为整体来处理的。
//  * 7）mapPartitionsWithIndex
//  * mapPartitionsWithSplit与mapPartitions的功能类似， 只是多传入split index而已，所有func 函数必需是 (Int, Iterator<T>) =>
//  * Iterator<U> 类型。
//  *
//  * 7）sample
//  * sample(withReplacement,fraction,seed)是根据给定的随机种子seed，随机抽样出数量为frac的数据。withReplacement：是否放回抽
//  * 样；fraction：比例，0.1表示10% ；
//  * val a = sc.parallelize(1 to 10000, 3)
//  * a.sample(false, 0.1, 0).count
//  * res24: Long = 960
//  * 8）7）union
//  * union(otherDataset)是数据合并，返回一个新的数据集，由原数据集和otherDataset联合而成。
//  * val rdd8 = rdd1.union(rdd3)
//  * rdd8.collect
//  * res14: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 14, 16, 18)
//  * 8）intersection
//  * intersection(otherDataset)是数据交集，返回一个新的数据集，包含两个数据集的交集数据；
//  * val rdd9 = rdd8.intersection(rdd1)
//  * rdd9.collect
//  * res16: Array[Int] = Array(6, 1, 7, 8, 2, 3, 9, 4, 5)
//  *
//  * 9）distinct
//  * distinct([numTasks]))是数据去重，返回一个数据集，是对两个数据集去除重复数据，numTasks参数是设置任务并行数量。
//  * val rdd10 = rdd8.union(rdd9).distinct
//  * rdd10.collect
//  * res19: Array[Int] = Array(12, 1, 14, 2, 3, 4, 16, 5, 6, 18, 7, 8, 9)
//  *
//  * 10）groupByKey
//  * groupByKey([numTasks])是数据分组操作，在一个由（K,V）对组成的数据集上调用，返回一个（K,Seq[V])对的数据集。
//  * val rdd0 = sc.parallelize(Array((1,1), (1,2) , (1,3) , (2,1) , (2,2) , (2,3)), 3)
//  * val rdd11 = rdd0.groupByKey()
//  * rdd11.collect
//  * res33: Array[(Int, Iterable[Int])] = Array((1,ArrayBuffer(1, 2, 3)), (2,ArrayBuffer(1, 2, 3)))
//  *
//  * 11）reduceByKey
//  * reduceByKey(func, [numTasks])是数据分组聚合操作，在一个（K,V)对的数据集上使用，返回一个（K,V）对的数据集，key相同的值，
//  * 都被使用指定的reduce函数聚合到一起。
//  * val rdd12 = rdd0.reduceByKey((x,y) => x + y)
//  * rdd12.collect
//  * res34: Array[(Int, Int)] = Array((1,6), (2,6))
//  *
//  *
//  * 12）aggregateByKey
//  * aggreateByKey(zeroValue: U)(seqOp: (U, T)=> U, combOp: (U, U) =>U) 和reduceByKey的不同在于，reduceByKey输入输出都是(K,
//    * V)，而aggreateByKey输出是(K,U)，可以不同于输入(K, V) ，aggreateByKey的三个参数：
//  * zeroValue: U，初始值，比如空列表{} ；
//  * seqOp: (U,T)=> U，seq操作符，描述如何将T合并入U，比如如何将item合并到列表 ；
//  * combOp: (U,U) =>U，comb操作符，描述如果合并两个U，比如合并两个列表 ；
//  * 所以aggreateByKey可以看成更高抽象的，更灵活的reduce或group 。
//  * val z = sc.parallelize(List(1,2,3,4,5,6), 2)
//  * z.aggreate(0)(math.max(_, _), _ + _)
//  * res40: Int = 9
//  * val z = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3)))
//  * z.aggregateByKey(0)(math.max(_, _), _ + _)
//  * res2: Array[(Int, Int)] = Array((2,3), (1,9)
//    *
//    * 13）combineByKey
//  * combineByKey是对RDD中的数据集按照Key进行聚合操作。聚合操作的逻辑是通过自定义函数提供给combineByKey。
//  * combineByKey[C](createCombiner: (V) ⇒ C, mergeValue: (C, V) ⇒ C, mergeCombiners: (C, C)
//    * ⇒ C, numPartitions: Int):RDD[(K, C)]把(K,V) 类型的RDD转换为(K,C)类型的RDD，C和V可以不一样。combineByKey三个参数：
//  * val data = Array((1, 1.0), (1, 2.0), (1, 3.0), (2, 4.0), (2, 5.0), (2, 6.0))
//  * val rdd = sc.parallelize(data, 2)
//  * val combine1 = rdd.combineByKey(createCombiner = (v:Double) => (v:Double, 1),
//    * mergeValue = (c:(Double, Int), v:Double) => (c._1 + v, c._2 + 1),
//    * mergeCombiners = (c1:(Double, Int), c2:(Double, Int)) => (c1._1 + c2._1, c1._2 + c2._2),
//    * numPartitions = 2 )
//  * combine1.collect
//  * res0: Array[(Int, (Double, Int))] = Array((2,(15.0,3)), (1,(6.0,3)))
//  *
//  *  14）sortByKey
//  * sortByKey([ascending],[numTasks])是排序操作，对(K,V)类型的数据按照K进行排序，其中K需要实现Ordered方法。
//  * val rdd14 = rdd0.sortByKey()
//  * rdd14.collect
//  * res36: Array[(Int, Int)] = Array((1,1), (1,2), (1,3), (2,1), (2,2), (2,3))
//  *
//  * 15）join
//  * join(otherDataset, [numTasks])是连接操作，将输入数据集(K,V)和另外一个数据集(K,W)进行Join， 得到(K, (V,W))；该操作是对于相同K
//  * 的V和W集合进行笛卡尔积 操作，也即V和W的所有组合；
//  * val rdd15 = rdd0.join(rdd0)
//  * rdd15.collect
//  * res37: Array[(Int, (Int, Int))] = Array((1,(1,1)), (1,(1,2)), (1,(1,3)), (1,(2,1)), (1,(2,2)), (1,(2,3)), (1,(3,1)), (1,(3,2)), (1,(3,3)), (2,(1,1)),
//    * (2,(1,2)), (2,(1,3)), (2,(2,1)), (2,(2,2)), (2,(2,3)), (2,(3,1)), (2,(3,2)), (2,(3,3)))
//  * 连接操作除join 外，还有左连接、右连接、全连接操作函数： leftOuterJoin、rightOuterJoin、fullOuterJoin。
//  *
//  * 16）cogroup
//  * cogroup(otherDataset, [numTasks])是将输入数据集(K, V)和另外一个数据集(K, W)进行cogroup，得到一个格式为(K, Seq[V], Seq[W])
//  * 的数据集。
//  * val rdd16 = rdd0.cogroup(rdd0)
//  * rdd16.collect
//  * res38: Array[(Int, (Iterable[Int], Iterable[Int]))] = Array((1,(ArrayBuffer(1, 2, 3),ArrayBuffer(1, 2, 3))), (2,(ArrayBuffer(1, 2,
//    * 3),ArrayBuffer(1, 2, 3))))
//  *
//  * 17）cartesian
//  * cartesian(otherDataset)是做笛卡尔积：对于数据集T和U 进行笛卡尔积操作， 得到(T, U)格式的数据集。
//  * val rdd17 = rdd1.cartesian(rdd3)
//  * rdd17.collect
//  * res39: Array[(Int, Int)] = Array((1,12), (2,12), (3,12), (1,14), (1,16), (1,18), (2,14), (2,16), (2,18), (3,14), (3,16), (3,18), (4,12), (5,12),
//    * (6,12), (4,14), (4,16), (4,18), (5,14), (5,16), (5,18), (6,14), (6,16), (6,18), (7,12), (8,12), (9,12), (7,14), (7,16), (7,18), (8,14), (8,16),
//    * (8,18), (9,14), (9,16), (9,18))
//  *
//  * Spark RDD action操作
//    * 1）reduce
//  * reduce(func)是对数据集的所有元素执行聚集(func)函数，该函数必须是可交换的。
//  * val rdd1 = sc.parallelize(1 to 9, 3)
//  * val rdd2 = rdd1.reduce(_ + _)
//  * rdd2: Int = 45
//  *  2）collect
//  * collect是将数据集中的所有元素以一个array的形式返回。
//  * rdd1.collect()
//  * res8: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
//  *  3）count
//  * 返回数据集中元素的个数。
//  * rdd1.count()
//  * res9: Long = 9
//  * 4）first
//  * 返回数据集中的第一个元素， 类似于take(1)。
//  * rdd1.first()
//  * res10: Int = 1
//  *  5）take
//  * Take(n)返回一个包含数据集中前n个元素的数组， 当前该操作不能并行。
//  * rdd1.take(3)
//  * res11: Array[Int] = Array(1, 2, 3)
//  *  6）takeSample
//  * takeSample(withReplacement,num, [seed])返回包含随机的num个元素的数组，和Sample不同，takeSample 是行动操作，所以返回
//  * 的是数组而不是RDD ， 其中第一个参数withReplacement是抽样时是否放回，第二个参数num会精确指定抽样数，而不是比例。
//  * rdd1.takeSample(true, 4)
//  * res15: Array[Int] = Array(9, 5, 5, 6)
//  *
//  * 7）takeOrdered
//  * takeOrdered(n， [ordering])是返回包含随机的n个元素的数组，按照顺序输出。
//  * rdd1.takeOrdered(4)
//  * res16: Array[Int] = Array(1, 2, 3, 4)
//  *  8）saveAsTextFile
//  * 把数据集中的元素写到一个文本文件，Spark会对每个元素调用toString方法来把每个元素存成文本文件的一行。
//  *  9）countByKey
//  * 对于(K, V)类型的RDD. 返回一个(K, Int)的map， Int为K的个数。
//  *  10）foreach
//  * foreach(func)是对数据集中的每个元素都执行func函数
//    */
//}



