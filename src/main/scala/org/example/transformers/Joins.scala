package org.example.transformers

import org.apache.spark.sql.functions.{broadcast, col, count, lit, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.example.supportiveClasses.JoinTypes

class Joins(spark: SparkSession) {
  def joinTable(df: DataFrame): Unit= {

    val countryDF: Dataset[Row] = df.select(col("`Country/Region`"),
        col("`Deaths`"),
        monotonically_increasing_id().alias("id1")).
      filter(col("id1")%2 === 0)

    val whoRegionDF: Dataset[Row] = df.select(col("`WHO Region`"),
        monotonically_increasing_id().alias("id2")).
      limit(10)

    testDuplicateColumnExistence(df)
    testInnerJoin(countryDF, whoRegionDF)
    testOuterJoin(countryDF, whoRegionDF)
    testLeftJoin(countryDF, whoRegionDF)
    testRightJoin(countryDF, whoRegionDF)
    testLeftSemiJoin(countryDF, whoRegionDF)
    testLeftAntiJoin(countryDF, whoRegionDF)
    crossJoin(countryDF, whoRegionDF)
  }

  def testLeftAntiJoin(df1: Dataset[Row], df2: Dataset[Row]): Unit={
    println("<<< LEFT ANTI JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.
      join(df2, joinExpr, JoinTypes().leftAnti).
      show(false)
  }

  def testLeftSemiJoin(df1: DataFrame, df2: DataFrame): Unit={
    println("<<< LEFT SEMI JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.
      join(df2.filter(col("id2") =!= 8), joinExpr, JoinTypes().leftSemi).
      show(false)
  }

  def testRightJoin(df1: Dataset[Row], df2: Dataset[Row]): Unit={
    println("<<< RIGHT JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.
      join(df2, joinExpr, JoinTypes().rightOuter).
      show(false)
  }

  def testLeftJoin(df1: DataFrame, df2: DataFrame): Unit={
    println("<<< LEFT JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.
      join(broadcast(df2), joinExpr, JoinTypes().leftOuter).
      show(false)
  }

  def testOuterJoin(df1: Dataset[Row], df2: DataFrame): Unit={
    println("<<< OUTER JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.
      join(broadcast(df2), joinExpr, JoinTypes().outer).
      show(false)
  }

  def testInnerJoin(df1: DataFrame, df2: DataFrame): Unit={
    println("<<< INNER JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.join(df2, joinExpr,  JoinTypes().inner).
      show(false)
  }

  def crossJoin(df1: DataFrame, df2: DataFrame): Unit={
    println("<<< CROSS JOIN >>>")
    val joinExpr = df1.col("id1") === df2.col("id2")
    df1.join(df2, joinExpr,  JoinTypes().crossJoin).
      show(false)
  }

  def testDuplicateColumnExistence(df: DataFrame): Unit={
    val dummy = spark.
      range(df.count).
      withColumn("WHO Region", lit("WORLD"))

    val joinExpr = dummy.col("id") === col("newDF.id")

    df.
      withColumn("id", monotonically_increasing_id()).
      selectExpr("*").alias("newDF").
      join(broadcast(dummy), joinExpr, "inner").
      show(false)
  }
}
