#  RDD编程模型介绍

## 什么是RDD

> 弹性式分布式数据集。Spark中计算的基本单元。分布式存储提升了RDD的读写性能，弹性表示内存充足数据就存储在内存中，内存不足，就存储在磁盘中。
>
> 	一组RDD可以形成DAG（有向(方向)无环图）
>
> 	RDD是Spark的第一代编程模型。

## Spark的持久化设置

> 	一般来说将数据从内存中保存到磁盘中就叫做持久化。从某些场景下，将数据从内存中溢出到缓存中也叫做持久化。
>
> 	在RDD中有一个方法persist或cache，他们的作用就是标记RDD持久化。
>
> 	persist是持久化数据到磁盘中，可以选择不同的持久化策略

| 持久化策略          | 含义                                                         |
| ------------------- | ------------------------------------------------------------ |
| MEMORY_ONLY（默认） | RDD中的数据以未经过序列化的Java对象的形式保存在内存中。如果内存不足，剩余的部分不会持久化，使用的时候， 没有持久化的部分重新加载。这种效率还可以。 |
| MEMORY_ONLY_SER     | 相比较MEMORY_ONLY，多了一个序列化功能。他会对内存中的数据经过序列化为一个字节数组。 |
| MEMORY_AND_DISK     | 弹性表示内存充足数据就存储在内存中(在内存中的时候没有经过序列化)，内存不足，就存储在磁盘中 |
| MEMORY_AND_DISK_SER | 弹性表示内存充足数据就存储在内存中(在内存经过序列化)，内存不足，就存储在磁盘中 |
| DISK_ONLY           | 只存储在磁盘中                                               |
| xxx_2               | 上述的策略后面加上_2。比如：MEMORY_ONLY_2。`_2`表示的副本。性能肯定下降，但是安全性能提升。一般不用 |
| HEAP_OFF            | 使用堆外内存(非spark内存)，可以spark数据存储到：Redis、HBase、ClickHouse。。。 |

```scala
package com.qf.bigdata.spark.core.day4

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Demo6_Persist {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    var start = System.currentTimeMillis()
    val textRDD: RDD[String] = sc.textFile("C:\\ftp\\SZBIGDATA-2103\\day3-flink旅游\\resource\\QParameterTool.java")
    var count: Long = textRDD.count()
    println(s"没有持久化 lines count : ${count}, cost time : ${System.currentTimeMillis() - start} ms")

    textRDD.persist(StorageLevel.MEMORY_AND_DISK) // 开启持久化
      start = System.currentTimeMillis()
      count = textRDD.count()
      println(s"持久化 lines count : ${count}, cost time : ${System.currentTimeMillis() - start} ms")
    textRDD.unpersist() // 卸载持久化

    SparkUtils.close(sc)
  }
}
```

## DAG ：有向无环图

![](./src/im017.png)

##  共享变量

> 所谓的共享变量就是大家都能使用的变量。那这个“大家”指什么东西？指的是执行器(executor)

## 广播变量

![](018.png)

```scala
package com.qf.bigdata.spark.core.day4

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Demo7_Broadcast {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    //1. 加载数据
    val stuRDD = sc.parallelize(List(
      Student("1", "令狐冲", "1", 11),
      Student("2", "任盈盈", "0", 12),
      Student("3", "岳灵珊", "0", 13),
      Student("4", "东方姑娘", "2", 14),
    ))
    val genderMap = Map(
      "0" -> "小姐姐",
      "1" -> "小哥哥"
    )
    //2. 经过分析，我们stuRDD在执行的时候会被分成多个task执行，需要genderMap多次，这样增大了传输，所以我们需要对他进行广播
    val genderBC: Broadcast[Map[String, String]] = sc.broadcast(genderMap)
    //3. 处理数据
    val resRDD: RDD[Student] = stuRDD.map(stu => {
      //3.1 从广播变量中获取到性别的值
      val gender: String = genderBC.value.getOrElse(stu.gender, "人妖")
      Student(stu.id, stu.name, gender, stu.age)
    })
    resRDD.foreach(println)
    SparkUtils.close(sc)
  }
}

case class Student(id:String, name:String, gender:String, age:Int)
```

### 累加器：类似于Mapreduce学习计数器(accumulator)

```scala
package com.qf.bigdata.spark.core.day4

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

object Demo8_Accumulator {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    val stuRDD = sc.parallelize(List(
      "Our materialistic society to led us to believe that to cannot be obtained without having money."
    ))

    // 统计to这个单词出现了几次
    val count: Long = stuRDD.flatMap(_.split("\\s+")).filter(word => word == "to").count()
    println(count)

    // 利用累加器来统计
    // 获取到累加器
    val accumulator: LongAccumulator = sc.longAccumulator
    stuRDD.flatMap(_.split("\\s+")).filter(word => {
      val isok = word == "to"
      if (isok) accumulator.add(1)
      isok
    }).foreach(println)
    println(accumulator.value)

    SparkUtils.close(sc)
  }
}
```
##RDD中的分区
> ​	为了保证RDD中的并行读写，从而保证数据处理的速度，我们在RDD中有分区的概念，一个完整的数据RDD中被分为了多个分区存储。
>
> ​	对于数据分拆之后究竟存储于哪一个分区，是由分区器（Partitioner）决定。在Spark中默认提供了集中分区器，我们也可以自定义分区器。
>
> ​	Spark中目前支持的分区器：Hash分区和Range分区。

### 5.1 Partitioner

![](019.png)

###HashPartitioner

> 90%以上RDD的相关分区默认都是使用的这个分区器。
>
> 作用：依据RDD中的key值进行hashcode，然后对整个分区数取模，得到paritionId值。同时支持key为null的情况，如果key为null将把这条数据存储到0分区中。

###自定义Partitioner

> 根据自己的需求，量身定制的数据应该存储到哪一个分区中。

###CustomerPartitioner

```scala
package com.qf.bigdata.spark.core.day5

import org.apache.spark.Partitioner

import scala.util.Random

/**
 * 随机分区器
 */
class CustomerPartitioner(partitions: Int) extends Partitioner{

  private val random = new Random()

  /**
   * 表示自定义分区器的分区数总共是多少
   */
  override def numPartitions: Int = partitions

  /**
   * 根据key值分配数据到哪个分区中
   */
  override def getPartition(key: Any): Int = random.nextInt(numPartitions)
}

```

####Demo1_Partitioner

```scala
package com.qf.bigdata.spark.core.day5

import com.qf.bigdata.spark.core.day3.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}

object Demo1_Partitioner {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = SparkUtils.getLocalSparkContext()

    //1. 加载数据，并处理数据为一个二维的元祖
    val listRDD: RDD[(Int, Long)] = sc.parallelize(1 to 10, 4)
      .zipWithIndex() // [(1,0),(2,1),(3,2),...,(10,9)]

    //2. 如果我们不设置自定义分区器，默认使用的是hash分区
    println("默认使用hashpartitioner ------------------------------------")
    val func = (partitionId:Int, iterator:Iterator[(Int, Long)]) => iterator.map(t => {
      s"[partition : ${partitionId}, value : ${t}]"
    })

    var arrays: Array[String] = listRDD.mapPartitionsWithIndex(func).collect()
    arrays.foreach(println)

    println("自定义分区器 ------------------------------------")
    //3. 注册自定义分区器
    val rdd2: RDD[(Int, Long)] = listRDD.partitionBy(new CustomerPartitioner(4))
    arrays = rdd2.mapPartitionsWithIndex(func).collect()
    arrays.foreach(println)

    SparkUtils.close(sc)
  }
}
```

布置作业：2个分区，1to10000，要求奇数分一个区，偶数分一个区
