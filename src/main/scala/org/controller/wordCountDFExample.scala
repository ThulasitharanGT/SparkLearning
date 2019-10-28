package org.controller
import org.apache.spark.sql.functions._

import org.util.SparkOpener

object wordCountDFExample extends SparkOpener {
  val spark=SparkSessionLoc("wordCountExample")

  def main(args: Array[String]): Unit =
  {
  val input=spark.read.format("com.databricks.spark.csv").load("C:\\Users\\RAPTOR\\IdeaProjects\\SparkLearning\\Input\\shakesphere.txt")
  val output=input.selectExpr("explode(split(LOWER(_c0),' '))").groupBy("col").agg(count("col"))
  output.show()
  }
}