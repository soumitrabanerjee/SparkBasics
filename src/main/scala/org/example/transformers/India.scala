package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, expr}

class India {
  def covidInIndia(indiaDF: DataFrame): Unit={
    val filterDeaths = col("Deaths") > 100 && col("Deaths") < 1000
    val sortExpr = desc("Deaths")
    val exp = expr("`Country/Region` > 200")

    val deathsDF = indiaDF.
      select("1 week change", "Deaths").
      filter(filterDeaths).
      sort(sortExpr).
      filter(exp)

    deathsDF.explain
  }
}
