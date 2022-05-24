package SparkOperator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/23 19:11
 * @Version 1.0
 */
/* spark算子练习，mappartititons算子，是map算子的升级版和批处理版，
 *以分区为单位执行map，有批量的感觉
 * 此算子一个分区作为一个批次来进行数据处理
 */
object DemoMapPartitions {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "demo_map", new SparkConf)
    val value = sc.parallelize(List
      ((1, "ma"),
      (2, "lihua"),
      (3, "mengmeng"),
        (3, "ha")
    ))
    //调用map参数时map参数是一个函数
    value.map(_._1)
    //调用mapPartitions时mapPartitions中的参数是一个迭代器，
    // 迭代器代表分区，需要对迭代器中的元素再调用map函数
    value.mapPartitions(_.map(_._1)).foreach(println)
  }

}
