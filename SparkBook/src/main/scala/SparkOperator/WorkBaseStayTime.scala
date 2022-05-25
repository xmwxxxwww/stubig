package SparkOperator

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/25 15:33
 * @Version 1.0
 */
object WorkBaseStayTime {
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "WorkBaseStayTime", new SparkConf)
    val logLineRdd = sc.textFile("./src/dateFile/sparkOperatorDataFile/19735E1C66.log")
    val sqlLineRdd = sc.textFile("./src/dateFile/sparkOperatorDataFile/lac_info.txt")
    logLineRdd.map(lines => (
      lines.split(",")

    ))
    println("hahahahaha"*10)
    sqlLineRdd.map(lines => (
      lines.split(",")
    ))
  }
}
