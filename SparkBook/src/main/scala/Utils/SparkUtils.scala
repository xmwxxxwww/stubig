package Utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.xbill.DNS.Master

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/27 09:54
 * @Version 1.0
 */
object SparkUtils {
/*自定义spark类*/
  object SparkUtils{
  /*
  * 获取本地的SparkContext对象
  * */
  //此方法只可以更改appName的参数
  def getLocalSparkContext():SparkContext = getLocalSparkContext("default_app")

//  获取到本地的SparkContext对象调用getSparkContext(master:String,appName:String)方法//此方法可以更改master和appName的方法
  def getLocalSparkContext(appName:String) : SparkContext = getSparkContext("local[*]",appName)

//  获取到SparkContext对象调用SparkContext方法，此方法可以修改全部参数
  def getSparkContext(master:String,appName:String) = new SparkContext(master ,appName,new SparkConf())

//  获取到SparSession对象，调用getLocalSparkSession(appName:String,isSupportedHive:Boolean = false)此方法可以修改appName和是否支持hive，默认为false不支持
  def getLocalSparkSession():SparkSession = getLocalSparkSession("default_app",false)
//  获取SparkSession对象，调用getSparkSession(master: String , appName: String, isSupportedHive: Boolean = false)方法可以通过参数修改appName和是否支持hive以及master
  def getLocalSparkSession(appName:String,isSupportedHive:Boolean = false):SparkSession = getSparkSession("Local[*]",appName,isSupportedHive)
  //获取SparkSession方法
  def getSparkSession(master: String , appName: String, isSupportedHive: Boolean = false)={
    if(isSupportedHive) SparkSession.builder().appName(appName).master(master).enableHiveSupport().getOrCreate()
    else SparkSession.builder().appName(appName).master(master).getOrCreate()
  }
//  释放资源，释放
  def close(sc:SparkContext):Unit = if (sc != null && !sc.isStopped) sc.stop()
  def close(spark:SparkSession) :Unit = if(spark != null && !spark.sparkContext.isStopped) spark.stop()

}

}
