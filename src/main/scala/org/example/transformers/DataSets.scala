package org.example.transformers

import org.apache.spark.sql.functions.{col, lit, map, when}
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.example.supportiveClasses.{JoinTest, MyCaseClass}

import java.io.Serializable

class DataSets(spark: SparkSession) extends Serializable {

  def getBanerjeeTitle(myInfo: MyCaseClass): Boolean={
    if(myInfo.name.toLowerCase.contains("banerjee")){true}
    else {false}
  }

  def myDataset(): Unit={
    import spark.implicits._
    val data = Seq(Row("Soumitra Banerjee", 1, "Amex", 20.toByte),
      Row("Somrita Mukherjee", 2, "Accenture", 30.toByte),
      Row("Radhe Shyam", 3, "Star Cement", 40.toByte),
      Row("Soumitra TEST", 1, "Amex", 20.toByte)
    )

    val mySchema = new StructType(Array(
      StructField("name", StringType, true),
      StructField("id", IntegerType, true),
      StructField("company", StringType, true),
      StructField("bytes", ByteType, true))
    )

    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, mySchema)
    val newDS = df.as[MyCaseClass]

    newDS.filter(row => getBanerjeeTitle(row)).show(false)

    val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong)).
      withColumnRenamed("_1", "id").
      withColumnRenamed("_2", "randomData").
      as[JoinTest]

    val df1 = newDS.joinWith(flightsMeta, newDS.col("id")===flightsMeta.col("id"))
    df1.printSchema
    df1.selectExpr("_1.company").show

    val df2 = newDS.join(flightsMeta, Seq("id"))
  }
}
