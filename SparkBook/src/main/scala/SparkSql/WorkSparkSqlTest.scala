package SparkSql

import org.apache.log4j.{Level, Logger}

/**
 * @Author dododo 
 * @Description TODO
 * @Date 2022/5/26 20:11
 * @Version 1.0
 */
/*1. 统计出每一个省份广告被点击次数的 TOP3
* # 数据结构：时间戳，省份，城市，用户，广告 ，样本如下，字段使用空格分割
* 1516609143867 6 7 64 16
* 1516609143869 9 4 75 18
* 1516609143869 1 7 87 12*/
object WorkSparkSqlTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

  }

}
