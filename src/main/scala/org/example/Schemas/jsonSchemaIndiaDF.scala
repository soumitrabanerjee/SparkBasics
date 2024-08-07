package org.example.Schemas

import org.apache.arrow.vector.types.pojo.ArrowType.ComplexType
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object jsonSchema {
  val myStructSchema = StructType(Array(
    StructField("Country/Region", StringType, true),
    StructField("Deaths", IntegerType, true),
    StructField("WHO Region", StringType, true),
    StructField("complex_arr", ArrayType(StringType, true), true)
  ))
}
