package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author dododo
 * @Description TODO
 * @Date 2022/5/27 19:40
 * @Version 1.0
 */
object DemoCreateDS2DF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import  spark.implicits._
    val stuRDD = spark.sparkContext.parallelize(List(
      Stu(1, "王东阳", "男", 18),
      Stu(2, "王西阳", "女", 19),
      Stu(3, "王北阳", "男", 20),
      Stu(4, "王南阳", "女", 21)
    ))
    val ds: Dataset[Stu] = stuRDD.toDS()
    val df: DataFrame = ds.toDF()
    df.show()
  }


}
case class Stu(id:Int,name:String,gender:String,age:Int)