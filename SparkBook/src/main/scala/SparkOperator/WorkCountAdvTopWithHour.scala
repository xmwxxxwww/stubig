package SparkOperator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/25 11:31
 * @Version 1.0
 */
object WorkCountAdvTopWithHour {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "Work_Count_Adv_Top_With_Hour", new SparkConf)
//  加载数据
    val advRDD = sc.textFile("./src/dataFile/sparkDataFile/sparkOperatorDataFile/Adver.txt")
//    advRDD.foreach(println)
//   对数据进行处理，取出（时间，省份，广告id）的元素
    val tupe_HH_Sp_Adv = advRDD.map(line => {
      val strings = line.split(" ")
      val formatHH = new SimpleDateFormat("yyyy-MM-dd HH")
      val da = formatHH.format(new Date(strings(0).toLong))
      (da, strings(1), strings(4))
    })
//
  }

}
