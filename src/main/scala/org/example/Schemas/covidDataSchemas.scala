package org.example.Schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object covidDataSchemas {
  val countrySchema: StructType = StructType(Array(
    StructField("Country/Region", StringType, true),
    StructField("Confirmed", IntegerType, true),
    StructField("Deaths", IntegerType, true),
    StructField("Recovered", IntegerType, true),
    StructField("Active", IntegerType, true),
    StructField("New cases", IntegerType, true),
    StructField("New deaths", IntegerType, true),
    StructField("New recovered", IntegerType, true),
    StructField("Deaths / 100 Cases", DoubleType, true),
    StructField("Recovered / 100 Cases", DoubleType, true),
    StructField("Deaths / 100 Recovered", StringType, true),
    StructField("Confirmed last week", IntegerType, true),
    StructField("1 week change", IntegerType, true),
    StructField("1 week % increase", DoubleType, true),
    StructField("WHO Region", StringType, true)
  ))
}
