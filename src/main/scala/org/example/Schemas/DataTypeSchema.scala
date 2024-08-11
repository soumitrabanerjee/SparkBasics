package org.example.Schemas

import org.apache.spark.sql.types.{ArrayType, BinaryType, ByteType, StringType, StructField, StructType}

object DataTypeSchema {
  val dataTypeSchema = new StructType(Array(
    StructField("Byte_Type_Data", ByteType, true),
    StructField("Binary_Type_Data", BinaryType, true),
    StructField("Array_Type_Data", ArrayType(StringType, true), true)
  ))
}
