package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 17:23
 * @Version 1.0
 */
/*
* RDD转换为dataset 使用toDS方法可以直接将List集合中华的数据转为dataset格式的数据
*
* */
object DemoCreateRDD2DS2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import spark.implicits._
    val stuRDD = spark.sparkContext.parallelize(List(
      Stua("1", "张三", 12),
      Stua("2", "李四", 15),
      Stua("3", "王五", 18)
    ))
    val ds = stuRDD.toDS()
    ds.show()
    spark.stop()
  }
}
case class Stua(id:String,name:String,age:Int)