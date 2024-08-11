package org.example.transformers

import org.apache.spark.sql.{DataFrame, SparkSession}

class FileTypes(spark: SparkSession) {

  def getParquetFile(df: DataFrame): Unit={
    df.
      write.
      format("parquet").
      partitionBy("WHO Region").
      mode("overwrite").
      option("compression","gzip").
      save("/Users/soumitrabanerjee/Desktop/SparkBasics/data/files.parquet")

    println("<<< PARQUET FILE >>>")
    spark.read.format("parquet").load("/Users/soumitrabanerjee/Desktop/SparkBasics/data/files.parquet").show(false)
    getORCFile(df)
  }

  def getORCFile(df: DataFrame): Unit={
    df.
      write.
      format("orc").
      partitionBy("WHO Region").
      mode("overwrite").
      save("/Users/soumitrabanerjee/Desktop/SparkBasics/data/files.orc")

    println("<<< ORC FILE >>>")
    spark.read.format("orc").load("/Users/soumitrabanerjee/Desktop/SparkBasics/data/files.orc").show(false)
  }

  def getJSONFile(): Unit={
    val df = spark.
      read.
      format("json").
      option("header", true).
      load("/Users/soumitrabanerjee/Desktop/SparkBasics/data/json/iris.json")

    df.limit(10).show(false)
  }
}
