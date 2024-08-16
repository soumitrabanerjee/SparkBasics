package org.example.transformers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.{Row, SparkSession}
import org.example.Schemas.DataTypeSchema

class DataTypes(spark: SparkSession) {
  def differentDataTypes(): Unit={
    val rowData = Seq(
      Row(-130.toByte, Array[Byte](-128), Array("soumitra", "somrita", ("hebby moja"))),
      Row(127.toByte, Array[Byte](5), Array("banerjee", "mukherjee", "hebby moja"))
    )

    val parallelisedData = spark.sparkContext.parallelize(rowData)
    val df = spark.createDataFrame(parallelisedData, DataTypeSchema.dataTypeSchema)
    val newDF = df.withColumn("new", explode(split(col("`Array_Type_Data`")(2), " ")))
    newDF.show(false)
    newDF.printSchema
  }
}
