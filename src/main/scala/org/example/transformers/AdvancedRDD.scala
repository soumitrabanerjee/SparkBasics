package org.example.transformers

import org.apache.spark.sql.SparkSession

case class simpleRdd(num: Int)

class AdvancedRDD(spark: SparkSession) extends Serializable {
  def getFirstDigit(x: Int): Int={
    var quot: Int = x
    while(quot>=0){
      quot = quot/10
    }
    return quot
  }

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

    // Aggregations
    println("\nAGGREGATIONS")
    val chars = words_rdd.flatMap(word => word.toLowerCase().toSeq)
    val KVCharacters = chars.map(letter => (letter, 1))
    def maxFunc(a: Int, b: Int):Int={Math.max(a,b)}
    def addFunc(a: Int, b: Int): Int={a+b}

    //countByKey
    val countByKey = KVCharacters.countByKey()
    println("\ncountByKey: " + countByKey)
    val countByValue = KVCharacters.countByValue()
    println("\ncountByValue: " + countByValue)

    //groupByKey
    val groupByKey = KVCharacters.groupByKey().map(row => (row._1, row._2.reduce(addFunc))).collect()
    for(elem <- groupByKey){println("groupByKey: " + elem)}

    //reduceByKey
    val reduceByKey = KVCharacters.reduceByKey(addFunc).collect()
    for(item <- reduceByKey){println("reduceByKey: " + item)}

    //OPERATIONS ON RDD
    import spark.implicits._
    println("OPERATIONS ON RDD")
    val ds = spark.range(2, 100, 4).toDF.as[simpleRdd]
    val rdd = spark.sparkContext.parallelize(ds.collect())
    val keywords = rdd.keyBy(x => getFirstDigit(x.num))
    keywords.keys.collect().foreach(println)
    keywords.values.collect().foreach(println)
    keywords.foreach(println)
  }
}