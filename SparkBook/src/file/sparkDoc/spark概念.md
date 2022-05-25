# 一 Spark发展历史

> 2009年在伯克利大学研究型项目，2010年通过BSD许可协议正式对外发布（开源）。2012年Spark第一篇论文发布，发布了第一个正式版（Spark 0.6.0）。
>
> 2013年被纳入到apache的基金会中，随后发布了Spark Streaming、Spark Mllib(机器学习库)、Shark（Spark on Hadoop）。
>
> 2014年5月底发布Spark1.0.0发布。发布Spark Graphx（图计算）、Spark SQL代替Shark
>
> 2015年，推出DataFrame（spark第二代编程模型），Spark第一代编程模型使用的RDD。国内真正的开始普及Spark的就是这年。
>
> 2016年，推出DataSet（spark第三代编程模型）
>
> 2017年，推出Structured Streaming（结构流），spark streaming的升级版。
>
> 2018年，Spark 2.4.0将spark底层的通信框架akka替换成了netty。
>
> ...
>
> 2022年，Spark已经发展3.x.x版本

# 二 Spark学习什么

> Spark Core ： Spark核心API，提供DAG分布式内存计算框架
>
> Spark SQL ： 提供交互式查询API
>
> Spark Streaming/Structured Streaming ：实时流处理框架
>
> Spark ML : 机器学习API（在项目中用到）
>
> Spark Graphx ： 图计算

# 三 Spark的由来

- Hadoop缺点


- 不适合低延迟数据访问（Mapreduce的缺点）
- 无法高效存储大量小文件
- 不支持多用户写入以及任意修改文件


- Hadoop与Spark的关系与区别

|                  | **Hadoop（Mapreduce）**       | **Spark**                                                    |
| ---------------- | ----------------------------- | ------------------------------------------------------------ |
| **起源**         | 2005                          | 2009                                                         |
| **数据处理引擎** | Batch（批处理）               | Batch（批处理）                                              |
| **编程模型**     | Mapreduce                     | RDD（弹性分布式数据集）                                      |
| **内存管理**     | Disk Based（基于磁盘）        | JVM Managed                                                  |
| **延迟度**       | High                          | Medium                                                       |
| **吞吐量**       | Medium                        | High                                                         |
| **优化机制**     | Manual                        | Manual                                                       |
| **API**          | Low-Level(代码提供了底层功能) | High-Level（高级功能以及封装好了）                           |
| **流式处理支持** | NA（Storm）                   | Spark Streaming/Structured Streaming(spark的流式计算本质上还是批式计算) |
| **SQL支持**      | Hive、Impala                  | Spark SQL                                                    |
| **Graph支持**    | NA（pregel）                  | Graphx                                                       |
| **机器学习支持** | NA                            | SparkMllib                                                   |

- 处理流程比较

![img](https://cdn.nlark.com/yuque/0/2022/png/28792949/1652669944356-4a51e3ce-dcff-43f4-8d42-b35ffd68b77d.png)

- 总结

> 1. Spark把运算的中间数据存放在内存，迭代计算效率更高；Mapreduce的中间结果需要落地到磁盘，这样必然会有磁盘的io操作，非常的影响性能。
> 2. Spark容错性更高，通过弹性式分布式数据集RDD来实现高效容错。所谓弹性：就是空间足够就在内存中计算，空间不够就在磁盘中计算。如果一旦出现数据丢失，她可以利用DAG的血缘来做数据恢复。
> 3. Spark的通用性更好，Spark提供了非常多的算子来帮助我们进行数据分析，比如transformation和action算子API功能来完成大量的分析工作。除此之外它还包括流式计算、图计算、机器学习库中的功能，而Mapreduce只提供了map和reduce两种操作，对于流式计算也很匮乏。
> 4. Spark框架和生态更为复杂。首先有RDD、血缘lineage、执行时产生DAG（有向无环图）、stage划分等。很多时候spark job都需要根据不同业务场景进行调优从而达到性能要求；Mapreduce框架和生态就相对简单，对性能要求也较弱，但是想的运行还是较为稳定，适合长期后台运行。

## 1 Spark Core

> 实现了Spark的基本功能、包含了任务调度、内存管理、错误恢复、与存储系统交互等等模块。Spark Core还包含了RDD（弹性式分布式数据集）的API的定义

## 2 Spark SQL

> 是Spark用来操作结构化数据的程序包。通过Spark SQL的SQL功能，换言之就是使用SQL或HQL来进行数据的查询。Spark SQL支持多种数据源：比如hive、parquet、json等等。

## 3 Spark Streaming

> Spark提供对实时数据进行流式计算的组件。提供了操作流式计算的API，并且与Spark Core中RDD API高度对应。

## 4 Structured Streaming

> 结构流是构建在Spark SQL引擎上的可伸缩且容错的流式处理引擎（理解未spark streqaming的升级版）。可以实现低至100毫秒的低延迟流式处理。