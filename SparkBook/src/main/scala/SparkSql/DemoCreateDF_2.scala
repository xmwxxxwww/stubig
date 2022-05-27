package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 16:47
 * @Version 1.0
 */
object DemoCreateDF_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    import org.apache.spark
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import spark.implicits._
    val teaRDD = spark.sparkContext.parallelize(List(
      "1,苍老师,女",
      "2,王老师,男",
      "3,刘老师,男"
    ))
    //出现toDF爆红的Cannot resolve symbol toDF 查看import spark.implicits._该隐式转换时是否没有导入
    //toDF传入的参数是列名
    val df = teaRDD.map(_.split(",")).map(arr => (arr(0), arr(1), arr(2))).toDF("id", "name", "gender")
    df.show()
    spark.stop()
  }

}
