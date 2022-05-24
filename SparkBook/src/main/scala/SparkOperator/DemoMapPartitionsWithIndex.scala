package SparkOperator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/23 22:37
 * @Version 1.0
 */
/*
*   MapPartitionsWithIndex算子；
*   是MapPartitions的升级版本，比前者多以index
*
* */
object DemoMapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "damo_mappartitionwithindex", new SparkConf)
    //listRDD是一个迭代器
    val listRDD = sc.parallelize(1 to 10, 3)

    //listRDD.foreach(println)
    listRDD.map(_ * 10).foreach(println)
    println("-️"*20)
    listRDD.mapPartitionsWithIndex{
      case (index,partitons) =>{
        println(s"prtitions's id is ${index},partitions's data is ${partitons.mkString(",")}")
        partitons.map("*" * 10)
      }
    }.foreach(println)

  }

}
