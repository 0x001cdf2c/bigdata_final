/*
  这是一个示例程序
*/

import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("WordCount")
      .getOrCreate()
    
    val data = Seq("Hello Spark", "Hello World")
    val words = spark.sparkContext.parallelize(data)
      .flatMap(_.split(" "))
      .countByValue()
    
    words.foreach(println)
    spark.stop()
  }
}