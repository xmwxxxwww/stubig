package SparkSql

import org.apache.spark.sql.SparkSession

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 20:46
 * @Version 1.0
 */
object DemoSparkAndHive {
  def main(args: Array[String]): Unit = {
    //检验是否传入了俩参数
    if(args == null  || args.length != 2){
      println(
        """
          |Parameters error!!! | usage : xxx.jar <basic path> <info path>
          |""".stripMargin)
      System.exit(-1)
    }
    //将参数（文件1路径和文件2路径）
    val Array(basicPath,infoPath) = args
    //获取到sparksession对象
    val spark = SparkSession.builder().appName("DemoSparkAndHive").master("yarn").enableHiveSupport().getOrCreate()
    import spark.implicits._
    //创建数据库
    spark.sql(
      """
        |create database if not exists spark_sql
        |""".stripMargin)
    //切换数据库
    spark.sql(
      """
        |use spark_sql
        |""".stripMargin)
    //建表
    spark.sql(
      """
        |create table if not exists spark_sql.teacher_basic(
        |name String,
        |age Int,
        |married String,
        |classes Int
        |)
        |row format delimited fields terminated by ','
        |""".stripMargin)
    //建第二个表
    spark.sql(
      """
        |create table if not exists spark_sql.teacher_info(
        |name String,
        |hight Int,
        |weight Int
        |)
        |row format delimited fields terminated by ','
        |""".stripMargin)
    //导入数据
    spark.sql(
      s"""
         |load data inpath '${basicPath}' into table spark_sql.teacher_basic
         |""".stripMargin)
    spark.sql(
      s"""
         |load data inpath '${infoPath}' into table spark_sql.teacher_info
         |""".stripMargin)
    spark.sql(
      """
        |create if not exists table spark_sql.teacher
        |as
        |select
        |b.name,
        |b.age,
        |b.married,
        |b.classes,
        |i.height
        |from sprk_sql.teacher_basic b left join spark_sql.teacher_info i
        |on b.name = i.name
        |""".stripMargin)
    spark.stop()
  }

}
