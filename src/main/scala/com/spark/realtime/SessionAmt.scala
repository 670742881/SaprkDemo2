package com.spark.realtime



import com.spark.common.TimeUtil
import com.spark.realtime.utilS.SparkUtils
import org.apache.spark.rdd.RDD



/**
  * @created by imp ON 2018/2/14
  */
object SessionAmt{
  def main(args: Array[String]): Unit = {
    val appName=this.getClass.getName
    val isLocal=true
    val conf = SparkUtils.generateSparkConf(appName, isLocal, that => {
      // 这里可以单独的指定当前spark应用的相关参数
      // nothings
      that.set("","")
    })
    // 2.3 SparkContext对象的构建
    val sc = SparkUtils.getSparkContext(conf)
    //可单独写方法判断 读取hdfs的某一天文件夹下所以文件
    val  path="data/test"
    val text=sc.textFile(path)
    //使用解析日志方法
    val log=SparkUtils.LogPrase(text)
    log.cache()

    val sessionID2RecordsRDD: RDD[(String, Iterable[Map[String, String]])] =log.map(i=>(i("sessionId"),i)).groupByKey()

    // 四、需求一代码
    /**
      * 用户的session聚合统计
      * 主要统计两个指标：会话数量&会话长度
      * 会话数量：sessionID的数量
      * 会话长度：一个会话中，最后一条访问记录的时间-第一条记录的访问数据
      * 具体的指标：
      * 1. 总的会话个数：过滤后RDD中sessionID的数量
      * 2. 总的会话长度(单位秒): 过滤后RDD中所有session的长度的和
      * 3. 无效会话数量：会话长度小于1秒的会话id数量
      * 4. 各个不同会话长度区间段中的会话数量
      * 0-4s/5-10s/11-30s/31-60s/1-3min/3-6min/6min+ ==> A\B\C\D\E\F\G
      * 5. 计算各个小时段的会话数量
      * 6. 计算各个小时段的会话长度
      * 7. 计算各个小时段的无效会话数量
      *
      * 注意：如果一个会话中，访问数据跨小时存在，eg：8:59访问第一个页面,9:02访问第二个页面；把这个会话计算在两个小时中(分别计算)
      **/
    val totalSessionCount: Long = sessionID2RecordsRDD.count()
    val sessionID2LengthRDD: RDD[(String, Long)] = sessionID2RecordsRDD.map {
      case (sessionID, recordsMap) => {  //使用case进行部分匹配也可以
        // 1. 获取当前会话中的所有数据的访问时间戳(毫米级)
        //Iterable（1200,1290,1499,1500）
        val actionTimestamps: Iterable[Long] = recordsMap.map(record => {
          // actionTime格式为yyyy-MM-dd HH:mm:ss的时间字符串
          val actionTime = record("s_time")
          val timestamp: Long = TimeUtil.parseNginxServerTime2Long(actionTime)
          timestamp
        })

        // 2. 获取第一条数据的时间戳(最小值)和最后一条数据的时间戳(最大值)
        val minActionTimestamp = actionTimestamps.min  //用户第1次访问网站的时间戳
        val maxActionTimestamp = actionTimestamps.max //用户离开网站时的时间戳
        val length = maxActionTimestamp - minActionTimestamp

        // 3. 返回结果

        (sessionID, length)
      }
    }
    //(zhangsan,90000),(lisi,1000000)
    sessionID2LengthRDD.cache()
    //以秒为单位的会话总时长 秒的单位
    val totalSessionLength: Double = sessionID2LengthRDD.map(_._2).sum() / 1000
    //无效会话的总个数 小于1秒的
    val invalidSessionCount: Long = sessionID2LengthRDD.filter(_._2 <1000).count()

    val preSessionLengthLevelSessionCount= sessionID2LengthRDD
      .map {
        case (_, length) => {
          // 根据会话长度得到当前会话的级别
          val sessionLevel = {
            if (length < 5000) "A"    //0-5s的会话
            else if (length < 11000) "B"   //5-11s会话
            else if (length < 31000) "C"
            else if (length < 60000) "D"
            else if (length < 180000) "E"
            else if (length < 360000) "F"
            else "G"
          }

          // 返回结果  （"A",1）,("B",1)
          (sessionLevel, 1)
        }
      }
      .reduceByKey(_ + _)   //（“A”，120），（“B”，2100）

    println("总会话个数========"+totalSessionLength)
    println("无效会话个数========"+invalidSessionCount)
"会话长度等级"+preSessionLengthLevelSessionCount.foreach(println(_))
//    sessionID2LengthRDD.unpersist()

  }







}
