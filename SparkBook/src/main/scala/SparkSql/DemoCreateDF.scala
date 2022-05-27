package SparkSql


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.beans.BeanProperty


/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 15:47
 * @Version 1.0
 */
/*
*示例：用于创建dataframe数据，通过sparkCore产生RDD格式的数据，然后再通过createDataFrame方法将RDD格式数据转化为
* DataFrame格式；
* spark.createDataFrame(stuRDD, classOf[Student])方法的两个参数分别是RDD格式的数据和数据的类型
* ？尝试加载文本格式的数据试一试
* */
object DemoCreateDF {


  def main(args: Array[String]): Unit = {
    import org.apache.spark
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("appName").master("local[*]").getOrCreate()
    import spark.implicits._
    //创建scala的list存储数据
    val list  = List(
      new Student(1,"徐东东",18),
      new Student(1,"徐东西",17),
      new Student(1,"徐东上",16),
      new Student(1,"徐东下",15)
    )
    //2. rdd
    val stuRDD = spark.sparkContext.parallelize(list)
    //3.创建dataframe
    val df = spark.createDataFrame(stuRDD, classOf[Student])//classof 等同于Student.class底层使用的是反射

    //4.打印
    df.show()
  }



}
class Student extends Serializable {
  @BeanProperty var id : Int = _
  @BeanProperty var name : String = _
  @BeanProperty var gender : String = _
  @BeanProperty var age : Int = _

  def this(id:Int,name:String,age:Int){
    this()
    this.id = id
    this.name = name
    this.gender = gender
    this.age = age
  }
}