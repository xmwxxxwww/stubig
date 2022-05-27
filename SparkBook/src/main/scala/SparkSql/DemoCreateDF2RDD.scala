package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 18:56
 * @Version 1.0
 */
object DemoCreateDF2RDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import spark.implicits._
    val stuRDD = spark.sparkContext.parallelize(List(
      "1,苍老师,男",
      "2,王老师,女"
    ))
    val  df= stuRDD.map(_.split(",")).map(arr => (arr(0), arr(1), arr(2))).toDF("id", "name", "gender")
    val rdd = df.rdd
    rdd.foreach((row => {
      val id = row.getString(0)
      val name = row.getString(1)
      val gender = row.getString(2)
      println(s"${id}-->${name}-->${gender}")
    }))
    spark.stop()
  }

}
