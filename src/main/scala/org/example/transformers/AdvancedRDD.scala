package org.example.transformers

import org.apache.spark.sql.SparkSession

class AdvancedRDD(spark: SparkSession) {
  def rdd(): Unit={
    val words =
      """I am learning Spark.
        |Spark is a distributed computing engine.
        |It is very Efficient to perform Big Data tasks.
        |I love to work on Spark""".
      stripMargin.
      split(" ")
    val words_rdd = spark.sparkContext.parallelize(words, 8)
    val simpleKeyValue = words_rdd.map(x => (x.toUpperCase(), 1))
    simpleKeyValue.keys.collect()
    simpleKeyValue.values.collect()

    //Using keyBy

    val keyword = words_rdd.keyBy(word => word.charAt(0).toString.toLowerCase)
    val keyvalues = keyword.mapValues(values => values.toUpperCase())
    keyvalues.keys.collect()
    keyvalues.values.collect()
  }
}
