package org.example.transformers

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Row, SparkSession}
import org.example.Schemas.covidDataSchemas.countrySchema

class RDD(spark: SparkSession) extends Serializable {
  def startsWithI(individual: String): Boolean={
    individual.startsWith("I")
  }

  def reduceWordLength(leftWord: String, rightWord: String): String={
    if(leftWord.length > rightWord.length){leftWord}
    else {rightWord}
  }
  def rddData():Unit={
    // Reading data from a given String
    val data: Array[String] = "I am Soumitra Banerjee. I love Distributed Computing and I love Spark. I work at American Express as Data Engineer II.".
      split(" ")

    val rdd_data = spark.sparkContext.parallelize(data, 8) // because I have 8 cores CPU locally
    rdd_data.distinct().count()
    rdd_data.foreach(println)
    rdd_data.foreachPartition(x => println(x.length + " $$ " + x.mkString("|")))

    // KEY VALUE PAIRS OPERATIONS
    val keyword = rdd_data.keyBy(w => w.charAt(0).toString.toLowerCase)
    val modified_data = keyword.mapValues(value => value.toUpperCase())
    modified_data.keys.collect()
    modified_data.values.collect()

    rdd_data.getStorageLevel
    rdd_data.filter(word => startsWithI(word))

    // Reading document line by line
    val rdd_1 = spark.sparkContext.textFile("/Users/soumitrabanerjee/Desktop/SparkBasics/data/csv/country_wise_latest.csv", 8)
    rdd_1.foreachPartition(x => println(x.mkString.split(",").foreach(println)))
    rdd_1.flatMap(data  => data.toSeq)
    rdd_1.mapPartitions(part => Iterator[Int](2)).sum()
    rdd_1.reduce(reduceWordLength)


    // Given a document and a lookup data, if the total number of word match in the document is more than 30% than it's a valid document, else invalid
    val rdd_2 = spark.sparkContext.wholeTextFiles("/Users/soumitrabanerjee/Desktop/SparkBasics/data/csv/country_wise_latest.csv", 8)
    val collected_data = rdd_2.collect()
    val splitted_arr_length: Double = collected_data(0).toString.split(",").length
    val lookup_arr = "India,China,NO_COUNTRY_1,Japan,NO_COUNTRY_2,Germany,Ghana,Greece,Greenland,Grenada,Guatemala,Guinea,Guinea-Bissau,Guyana,Haiti,Holy See,Honduras,Hungary,Iceland".split(",")
    val matched_data = lookup_arr.filter(word => collected_data(0).toString.contains(word))
    val matched_data_length: Double = matched_data.length.toDouble
    if((matched_data_length/splitted_arr_length) > 0.3)
    {println((matched_data_length/splitted_arr_length) + ": VALID DOCUMENT")} else
    {println((matched_data_length/splitted_arr_length) + ": INVALID DOCUMENT")}


  }
}
