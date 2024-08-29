package org.example.transformers

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}
import org.example.supportiveClasses.binarySchema

class BinaryFile(spark: SparkSession) {
  def binary(): Unit={
    val schema = new StructType(
      Array(
        StructField("id", IntegerType, false),
        StructField("name", StringType, true),
        StructField("bin_file", BinaryType, true)
      )
    )

    val data_init = Array(
      Row(1, "Soumitra", Array[Byte](1,2,3)),
      Row(2, "Sumit", Array[Byte](4,5,6))
    )

    val data = spark.sparkContext.parallelize(data_init)

    import spark.implicits._
    val df = spark.createDataFrame(data, schema).as[binarySchema]
    df.show()

    val bin = spark.read.format("binaryFile").load("/Users/soumitrabanerjee/Desktop/SparkLearnings/data/binary/test.jpeg")
    bin.show()
  }
}
