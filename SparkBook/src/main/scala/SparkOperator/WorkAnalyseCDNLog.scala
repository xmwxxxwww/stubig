package SparkOperator
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/25 16:57
 * @Version 1.0
 */
object WorkAnalyseCDNLog  {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "Work_Analyse_CDN_Log", new SparkConf)
    val lineRDD = sc.textFile("./src/dataFile/sparkDataFile/sparkWorkTestDataFile/第四题数据-cdn.txt")
    //lineRDD.foreach(println)
    //第一问：1.计算独立IP数
    /*思路：
    *   1.数据处理获取ip
    *   2.将数据按照ip分组
    *   3.对分组后的数据的元组内容进行reduce聚合
    * */
    // 获取数据ip
    val ipAdd = lineRDD.map(lines => {
      val strings = lines.split("\\s+")
      (strings(0),1)
    })
    //println(ipAdd.collect().toList)
    //ipAdd.groupBy(_._1).foreach(println)
    //数据分组，并聚合
    val countIpAdd = ipAdd.groupBy(_._1).map(e => (
      (e._1, e._2.map(_._2).reduce(_ + _))
      ))
    countIpAdd.foreach{
      case (k,v) => println(s"ip${k} 的数量是：${v}")
    }
    println("hahaha"*20)
    countIpAdd.filter(_._2 == 1).foreach(println)
    /*2.统计每个视频独立IP数
    * 有时我们不但需要知道全网访问的独立IP数，更想知道每个视频访问的独立IP数
    * 1.计算思路:
    *   1.1筛选视频文件将每行日志拆分成 (文件名，IP地址)形式
    *   1.2按文件名分组，相当于数据库的Group by 这时RDD的结构为(文件名,[IP1,IP1,IP2,…])，这时IP有重复
    *   1.3将每个文件名中的IP地址去重，这时RDD的结果为(文件名,[IP1,IP2,…])，这时IP没有重复
    * 2.计算过程:
    *   filter(x=>x.matches(“.([0‐9]+).mp4.“)) 筛选日志中的视频请求 map(x=>getFileNameAndIp(x))
    *   将每行日志格式化成 (文件名，IP)这种格式 groupByKey() 按文件名分组，这时RDD 结构为 (文件名，[IP1,IP1,IP2….])，
    *   IP有重复 map(x=>(x.1,x.2.toList.distinct)) 去除value中重复的IP地址
    *   sortBy(.2.size,false) 按IP数排序 3.计算结果样例: 视频：141081.mp4 独立IP数:2393 视频：140995.mp4 独立IP数:2050
    * */
    println("hahaha"*20)
    //lineRDD.foreach(println)
    //对处理的数据再次进行分组，去重和统计
    val countDisIp = lineRDD.map(lines => {
      val lineArrayRDD = lines.split("\\s+")
      (lineArrayRDD(6), lineArrayRDD(0))
    })
      .groupBy(_._1).map(countMp4 => (
      (countMp4._1, countMp4._2.map(_._2))
      )).map(disIP => {
      val distinct = disIP._2.toList.distinct
      (disIP._1, distinct)
    })
    println("第二问"+"hahaha"*20)
    val disDouIpNum = countDisIp.map(countIpDis => {
      val stringToBooleanToInt = countIpDis._2.size
      (countIpDis._1, stringToBooleanToInt)
    })
    disDouIpNum.foreach{
      case(k,v) => println(s"电影：${k}的独立ip访问数量是：${v}")
    }
    /*3.统计一天中每个小时的流量
    * 有时我想知道网站每小时视频的观看流量，看看用户都喜欢在什么时间段过来看视频
    * 1.计算思路:
    *   1.1.将日志中的访问时间及请求大小两个数据提取出来形成 RDD (访问时间,访问大小)，这里要去除404之类的非法请求
    *   1.2.按访问时间分组形成 RDD （访问时间，[大小1，大小2，….]） 1.3.将访问时间对应的大小相加形成 (访问时间，总大小)
    * 2.计算过程:
    *   filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)) 过滤非法请求
    *   map(x=>getTimeAndSize(x)) 将日志格式化成 RDD(请求小时,请求大小)
    *   groupByKey() 按请求时间分组形成 RDD(请求小时，[大小1，大小2，….]) map(x=>(x._1,x._2.sum)) 将每小时的请求大小相加，
    *   形成 RDD(请求小时,总大小) 3.计算结果样例: 00时 CDN流量=14G 01时 CDN流量=3G 02时 CDN流量=5G
    * */
    println("第三问"+"hahaha"*20)
    val countSizeNN = lineRDD.map(processing => {
      val strings = processing.split("\\s+")
      val strings1 = strings(3).split(":")
      val visitSize = strings(8)
      (strings1(1), visitSize)
    }).groupBy(_._1).map(countSize => {
      val size = countSize._2.map(_._2.toInt).reduce(_ + _)
      (countSize._1.toInt, (size).toDouble / 1024 / 1024)
    })
    //println("第三问"+"hahaha"*20)
    //上面的结果无法通过RDD的sortBy()和sortByKey()进行排序，需要通过collec方法使用sorted或者sortedWith进行
    val value = countSizeNN.collect().sorted
    value.foreach{
      case (k,v) => println(s"${k}时间段的访问数据为：${v}G")
    }
    
  }
}
