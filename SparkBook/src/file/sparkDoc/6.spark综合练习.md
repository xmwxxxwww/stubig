# 七 综合练习

## 0 处理每次运行的时候日志问题

```scala
package com.qf.bigdata.spark.core.day5

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.slf4j.LoggerFactory

object Demo1_Partitioner {

  //1. 获取到一个叫做Demo1_Partitioner的日志处理器
  private val logger = LoggerFactory.getLogger(Demo1_Partitioner.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN) // 设置org包下的所有的日志打印都是WARN级别

    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    //1. 加载数据，并处理数据为一个二维的元祖
    val listRDD: RDD[(Int, Long)] = sc.parallelize(1 to 10, 4)
      .zipWithIndex() // [(1,0),(2,1),(3,2),...,(10,9)]

    //2. 如果我们不设置自定义分区器，默认使用的是hash分区
    println("默认使用hashpartitioner ------------------------------------")
    val func = (partitionId:Int, iterator:Iterator[(Int, Long)]) => iterator.map(t => {
      s"[partition : ${partitionId}, value : ${t}]"
    })

    var arrays: Array[String] = listRDD.mapPartitionsWithIndex(func).collect()
    arrays.foreach(println)

    println("自定义分区器 ------------------------------------")
    //3. 注册自定义分区器
    val rdd2: RDD[(Int, Long)] = listRDD.partitionBy(new CustomerPartitioner(4))
    arrays = rdd2.mapPartitionsWithIndex(func).collect()
    arrays.foreach(println)

    SparkUtils.close(sc)
  }
}

```



## 1 案例1

> 有数据格式如下：
>
> timestamp   province   city	userid	adid
> 时间点 	  省份	   城市	用户     广告
> 用户ID范围:0-99
> 省份,城市,ID相同:0-9
> adid:0-19
>
> 需求：
>
> 1.统计每一个省份点击TOP3的广告ID
> 2.统计每一个省份每一个小时的TOP3广告ID

```scala
package com.qf.bigdata.spark.core.day5

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * 案例1
 */
object Demo2 {
  private val logger = LoggerFactory.getLogger(Demo2.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    //1. 读取文件
    val logRDD: RDD[String] = sc.textFile("C:\\ftp\\ZZBIGDATA-2201\\day008-spark core\\resource\\Advert.txt")

    //2. 切割日志:并统计省份和广告对应的点击次数之和
    val resRDD: RDD[(String, Int)] = logRDD.map(_.split("\\s+"))
      .map(arr => (arr(1) + "_" + arr(4), 1)) // 先统计这个省份的广告被点击了多少次，所以我要先以省份和广告作为key来统计次数
      .reduceByKey(_ + _) // 对省份和广告为key进行求点击次数之和

    //3. 以点击次数降序排序然后取top3
    val res: collection.Map[String, List[(String, Int)]] = resRDD.map {
      case (pa, cnt) => {
        val param: Array[String] = pa.split("_") // 省份和广告的字符串数组
        (param(0), (param(1), cnt)) // (province, adid, cnt)
      }
    }.groupByKey() // [(province, [(adid1, cnt1), (adid2,cnt2), ...])]
      .mapValues(values => values.toList.sortWith((previous, next) => previous._2 > next._2) // 按照点击次数降序排序
        .take(3)).collectAsMap()  // 取前3名

    //4. 打印
    println(res)

    SparkUtils.close(sc)
  }
}

```

## 2 案例2

> 1. 数据格式
>
> - 19735E1C66.log 这个文件中存储着日志信息
>
> ```
> 手机号,时间戳,基站ID,连接状态(1连接 0断开)
> ```
>
> - lac_info.txt 这个文件中存储基站信息
>
> ```
> 基站ID, 经, 纬度 
> ```
>
> 2. 需求
>
> 根据用户产生日志的信息,在那个基站停留时间最长
> 求所用户经过的所有基站的范围内（经纬度）所停留时间最长的Top2
>
> 翻译：**求每个用户停留时长最长的两个基站的经纬度是多少**

### 2.1 DateUtils

```scala
package com.qf.bigdata.spark.core.day5

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 日期工具类
 */
object DateUtils {

  private val DATE_FORMAT:String = "yyyyMMddHHmmss"

  /**
   * 将date的字符串转换为Date类型
   */
  def dateStr2Date(dateStr: String, DATE_FORMAT: String): Date = {
    //1. 获取到日期格式化对象
    val format = new SimpleDateFormat(DATE_FORMAT)
    //2. 转换
    format.parse(dateStr)
  }

  /**
   * 将一个日期格式的字符串，转换为一个Long类型时间戳
   */
  def dateStr2Timestamep(dateStr: String): Long = {
      //1. 将date的字符串转换为Date类型
      val date:Date = dateStr2Date(dateStr, DATE_FORMAT)
      //2. 获取到date对应的时间戳
      date.getTime
  }

}

```

### 2.2 Demo3

```scala
package com.qf.bigdata.spark.core.day5

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

/**
 * 案例2
 */
object Demo3 {
  private val logger = LoggerFactory.getLogger(Demo3.getClass.getSimpleName)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    //1. 加载数据
    val logRDD: RDD[String] = sc.textFile("C:\\ftp\\ZZBIGDATA-2201\\day008-spark core\\resource\\lacduration\\19735E1C66.log")
    //2. 处理原始日志数据，将其转换为一个二维的元素((phone,lac), ts_long)
    val userInfoRDD: RDD[((String, String), Long)] = logRDD.map(line => {
      //2.1 切日志
      val fields: Array[String] = line.split(",")
      //2.2 获取到关键的字段
      val phone: String = fields(0) //手机号码
      val ts: Long = DateUtils.dateStr2Timestamep(fields(1)) // 时间戳
      val lac: String = fields(2) // 基站id
      val eventType: Int = fields(3).toInt // 连接或者断开的状态
      //3. 计算停留时间
      val ts_long = if (eventType == 1) -ts else ts
      //4. 封装数据为元祖
      ((phone, lac), ts_long)
    })

    //3. 计算每个用户在每个基站停留的总时长
    val lacAndPTRDD: RDD[(String, (String, Long))] = userInfoRDD.reduceByKey(_ + _)
      .map {
        case ((phone, lac), ts_long) => (lac, (phone, ts_long))
      }

    //4. 加载基站信息数据
    val lacInfoRDD: RDD[String] = sc.textFile("C:\\ftp\\ZZBIGDATA-2201\\day008-spark core\\resource\\lacduration\\lac_info.txt")
    val lacAndXYRDD: RDD[(String, (String, String))] = lacInfoRDD.map(line => {
      val fields: Array[String] = line.split(",")
      val lac: String = fields(0)
      val x: String = fields(1)
      val y: String = fields(2)
      (lac, (x, y))
    })

    //5. join : [(lac, [(phone, ts_long),(x, y)]),...]
    val joinRDD: RDD[(String, ((String, Long), (String, String)))] = lacAndPTRDD join lacAndXYRDD
    val phoneAndTXYRDD: RDD[(String, Long, String, String)] = joinRDD.map {
      case (lac, ((phone, ts), (x, y))) => (phone, ts, x, y)
    }
    //6. 转换 ： 按照phone分组
    val groupRDD: RDD[(String, Iterable[(String, Long, String, String)])] = phoneAndTXYRDD.groupBy(_._1)

    //7. 降序排序
    val sortRDD: RDD[(String, List[(String, Long, String, String)])] = groupRDD.mapValues(_.toList.sortBy(_._2).reverse)

    //8. 处理一下，value中的phone数据和时长数据，只保留xy
    val resRDD: RDD[(String, List[(String, String)])] = sortRDD.map(t => {
      val phone = t._1
      val filterList: List[(String, String)] = t._2.map(tp => {
        val x: String = tp._3
        val y: String = tp._4
        (x, y)
      })
      (phone, filterList)
    })

    //9 top2
    val resultRDD: RDD[(String, List[(String, String)])] = resRDD.mapValues(_.take(2))
    println(resultRDD.collect().toList)

    SparkUtils.close(sc)

  }
}
```v