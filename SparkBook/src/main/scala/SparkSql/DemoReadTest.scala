package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 20:27
 * @Version 1.0
 */
object DemoReadTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    val frame: DataFrame = spark.read.format("text").load("./src/dataFile/sparkDataFIle/sparkSqlDataFile/dailykey.txt")
    val frame1 = frame.toDF("time")
    frame1.show()
    val frame2 = spark.read.format("json").load("./src/dataFile/sparkDataFIle/sparkSqlDataFile/people.json")
    frame2.show()
    val frame3 = spark.read.parquet("./src/dataFile/sparkDataFIle/sparkSqlDataFile/sqldf.parquet")
    frame3.show()
    spark.stop()

  }

}
