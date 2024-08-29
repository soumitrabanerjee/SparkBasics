package org.example.App

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.example.Schemas.covidDataSchemas
import org.example.transformers.{AdvancedRDD, Aggregations, BinaryFile, DataSets, DataTypes, FileTypes, Joins, RDD, SparkDistributedVariables}

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
      load("/Users/soumitrabanerjee/Desktop/SparkLearnings/data/csv/country_wise_latest.csv")

    new Aggregations(spark).aggregationFunc(df)
    new Joins(spark).joinTable(df)
    new FileTypes(spark).getJSONFile()
    new FileTypes(spark).getParquetFile(df)
    new DataTypes(spark).differentDataTypes()
    new DataSets(spark).myDataset()
    new RDD(spark).rddData()
    new AdvancedRDD(spark).rdd()
    new SparkDistributedVariables(spark).broadcastVariableUsage()
    new BinaryFile(spark).binary()
  }
}