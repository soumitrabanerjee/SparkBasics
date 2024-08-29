package org.example.transformers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

case class test (id: Long, name: String)

object OperationOnRDD extends App() {
  val spark = SparkSession.builder().appName("").getOrCreate()
  def rddOperation():Unit={
//    import org.apache.spark.implicits._
    val df = spark.range(10, 100, 2).withColumn("name", lit("Soumitra"))//.as[test]
//    df.filter(x => x.id!=20).map(x => !x.name.equals("so")).show
    println("jhgh")
  }
}
