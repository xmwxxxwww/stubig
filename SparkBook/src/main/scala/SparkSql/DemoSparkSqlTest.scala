package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql._

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/26 15:17
 * @Version 1.0
 */
/*
* spark sql初体验，spark的函数用法和SQL语句用法简单示例
* */
object DemoSparkSqlTest {
  def main(args: Array[String]): Unit = {
    //对输出日志进行处理，输出几倍为WARN
    Logger.getLogger("org").setLevel(Level.WARN)
    //获取sparkSession，固定写法
    val sparkSession = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    //读取json数据,
    val frame: DataFrame = sparkSession.read.json("./src/dataFile/sparkDataFIle/sparkSqlDataFile/user.json")
    //输出结构
    frame.printSchema()
    //输出内容
    frame.show()
    //输出查询内容，类似与sql的select查询
    frame.select("name","gender")
    //导入隐式转换，用于使用$符号的类似于SQL聚合函数的功能
    import sparkSession.implicits._
    //sparkSql的select与其他方法的组合使用实现查询功能，
    frame.select($"name",($"age"+ 1)).show()
    //别名
    frame.select($"name",($"age"+ 1)).as("age").show()
    //分组与统计
    frame.select($"age").groupBy($"age").count().show()
    //where条件查询
    frame.select($"age").where($"age" > 30).show()


    //将加载的数据映射为一张临时表
    frame.createOrReplaceTempView("user")
    //直接使用SQL语句进行查询
    sparkSession.sql(
      s"""
         |select name,gender,age from user where age > 30
         |""".stripMargin
    )

    sparkSession.stop()
  }

}
