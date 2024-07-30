package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, expr, lit}
import org.example.Schemas.covidDataSchemas

object SparkBasics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.
      builder().
      appName("SparkBasics").
      getOrCreate()

    val df = spark.read.format("csv").
      schema(covidDataSchemas.countrySchema).
      option("header", true).
      load("/Users/soumitrabanerjee/Desktop/data/country_wise_latest.csv")

    val filterIndia = col("`Country/Region`") === "India"

    new India().covidInIndia(df.filter(filterIndia))
  }
}