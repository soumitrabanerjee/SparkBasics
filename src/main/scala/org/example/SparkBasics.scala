package org.example

import org.apache.spark.sql.SparkSession

object SparkBasics {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.
      builder().
      appName("SparkBasics").
      getOrCreate()
    val df = spark.range(100).toDF
    df.cache.count
    df.write.format("csv").save("/Users/soumitrabanerjee/Desktop/")
  }
}
