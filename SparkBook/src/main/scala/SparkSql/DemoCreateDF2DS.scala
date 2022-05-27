package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 17:36
 * @Version 1.0
 */
/*
* Dataframe - DataSet
*
* */

object DemoCreateDF2DS {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import  spark.implicits._
    val stuRDD = spark.sparkContext.parallelize(List(
      "1,苍老师,女",
      "2,小泽老师,女",
      "3,泷泽老师,女"
    ))
    val df = stuRDD.map(_.split(",")).map(arr => (arr(0),arr(1),arr(2))).toDF("id", "name", "gender")
    df.show()
    spark.stop()
  }

}
