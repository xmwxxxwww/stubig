package SparkOperator

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/24 11:01
 * @Version 1.0
 */

/*timestamp   province   city	userid	adid
* 时间点 	  省份	   城市	用户     广告
* 用户ID范围:0-99
* 省份,城市,ID相同:0-9
* adid:0-19
* 1.统计每一个省份点击TOP3的广告ID
2.统计每一个省份每一个小时的TOP3广告ID*/
object WorkeCountAdvTop3 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "count_adv_top3", new SparkConf)
    val lines = Source.fromFile("./src/dataFile/sparkDataFile/sparkOperatorDataFile/Adver.txt").getLines()
    //lines.foreach(println)
    //sc.parallelize(lines)
    /*1516609143869 1 7 87 12
      1516609143869 2 8 92 9
      1516609143869 6 7 84 24
      ...源数据*/
    val listline  = lines.map( e => {
      val strings = e.split(" ")
      (strings(0),strings(1),strings(2),strings(3),strings(4))

    }).toList
    val value = sc.parallelize(listline)
    //value.foreach(println)
    println("haha"*20)
    val value1 = value.map(e =>((e._2,e._5))).groupBy(_._1)
    value1.foreach(println)
    println("haha"*20)
    val value2 = value1.map(e => {
      val stringToTuples = e._2.map(e => (e._2, e._1))
      (e._1,stringToTuples)
    })
    value2.foreach(println)
    println("haha"*20)
    val value3 = value2.map(e => {
      val stringToTuples = e._2.groupBy(_._1)
      (e._1,stringToTuples)
    })
    value3.foreach(println)
    println("haha"*20)
    val value4 = value3.map(a => (a._1, a._2.map(e => {
      val tuple = (e._1, e._2.size)
      tuple
    }
    )
    ))
    value4.foreach(println)
    //value4.sortBy((_._2.map(_._2))
    val value5 = value4.map(e => {
      val reverse = e._2.toList.map(e => (e._2, e._1)).sorted.reverse
      (e._1, reverse.take(3))
      //(e._1,e._2.toList.map(_._1),reverse)
    })

    //value4.sortBy(te:(()))
    //value4.map(w => (w._1,w._2.take(3))).foreach(println)
    value5.foreach{
      case (k,List((n,s),(n1,s1),(n2,s2))) => println(s"${k}省的top3是：广告${3}的数量是${n},广告${s1}的数量是${n1},广告${s2}的数量是${n2}")
    }
  }
}




