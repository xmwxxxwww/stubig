package SparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.beans.BeanProperty

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 17:05
 * @Version 1.0
 */
object DemoRDD2DS {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import spark.implicits._
    val list = List(
      new Student1(1, "王小小","男", 17),
      new Student1(1, "王大大","男", 18),
      new Student1(1, "王中中","男", 19),
      new Student1(1, "王上上", "男",20),
      new Student1(1, "王下下", "男",11)
    )
    val stuRDD = spark.sparkContext.parallelize(list)
    val df = spark.createDataFrame(stuRDD, classOf[Student1])
    df.show()
    spark.stop()
  }
}
class Student1 extends Serializable {
  @BeanProperty var id : Int = _
  @BeanProperty var name : String = _
  @BeanProperty var gender : String = _
  @BeanProperty var age : Int = _

  def this(id:Int,name:String,gender:String,age:Int){
    this()
    this.id = id
    this.name = name
    this.gender = gender
    this.age = age
  }
}