package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 19:20
 * @Version 1.0
 */
object DemoCreateDS2RDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import spark.implicits._
    val stuRDD = spark.sparkContext.parallelize(List(
      Stu2(1, "王东阳", "男", 18),
      Stu2(1, "王西阳", "女", 18),
      Stu2(1, "王晚阳", "女", 18),
      Stu2(1, "王早阳", "男", 18),
      Stu2(1, "王南阳", "女", 18),
      Stu2(1, "王北阳", "男", 18)
    ))
    //rdd to ds
    val value: Dataset[Stu2] = stuRDD.toDS()
    //ds to rdd
    val rdd: RDD[Stu2] = value.rdd
    spark.stop()
  }

}
case class Stu2(id:Int,name:String,gender:String,age:Int)