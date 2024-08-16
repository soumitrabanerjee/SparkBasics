package org.example.transformers

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.example.supportiveClasses.DistributedCaseClass

import scala.util.Random

class SparkDistributedVariables(spark: SparkSession) extends Serializable {
  def broadcastVariableUsage(): Unit={
    val data = Seq(Row(1, "soumitra"), Row(2, "sumit"), Row(3, "abhijeet"), Row(4, "ashvani"), Row(5, "dinesh"))
    val schema = new StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true)
    ))
    val rddData = spark.sparkContext.parallelize(data)
    import spark.implicits._
    val df = spark.createDataFrame(rddData, schema).as[DistributedCaseClass]

    val keywords = df.rdd.map(x => (x.id, x))
    val keyvalues = keywords.mapValues(words => words.name.toUpperCase())

    println("\nKEYS")
    keyvalues.keys.foreach(println)

    println("\nVALUES")
    keyvalues.values.foreach(println)

    println("\nFOR EACH VALUE")
    keyvalues.foreach(println)

    println("\nGLOM")
    val arr = keyvalues.glom().collect()
    arr.foreach(x => for (elem <- x) {println(elem)})

    println("\nBROADCAST")
    val bc = spark.sparkContext.broadcast(keywords.collect())
    bc.value.foreach(println)

  }
}
