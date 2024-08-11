package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expr, lit, spark_partition_id}
import org.example.Schemas.covidDataSchemas
import org.example.transformers.{Aggregations, DataSets, DataTypes, FileTypes, Joins}

object SparkBasics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      enableHiveSupport().
      appName("SparkBasics").
      getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("csv").
      schema(covidDataSchemas.countrySchema).
      option("header", true).
      load("/Users/soumitrabanerjee/Desktop/SparkBasics/data/csv/country_wise_latest.csv")

    val filterIndia = col("`Country/Region`") === "India"

//    new India(spark).covidInIndia(df)
//    new Aggregations(spark).aggregationFunc(df)
//    new Joins(spark).joinTable(df)
//    new FileTypes(spark).getJSONFile()
//    new FileTypes(spark).getParquetFile(df)
//    new DataTypes(spark).differentDataTypes()
    new DataSets(spark).myDataset()
  }
}