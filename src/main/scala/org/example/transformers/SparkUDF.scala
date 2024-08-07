package org.example.transformers

object SparkUDF {
  def getSummation(a: Double, b: Double): Double = {
    a+b
  }
}
