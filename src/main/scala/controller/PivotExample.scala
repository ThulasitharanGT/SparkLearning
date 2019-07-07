package controller

import util.SparkOpener

object PivotExample extends SparkOpener {
  def main(args: Array[String]): Unit ={
  val spark = SparkSessionLoc("SparkSession")
  val df=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","|").load("D:\\study\\Car_pivot_example.txt")
  df.groupBy("brand","year").pivot("qtr").sum("profit").na.fill("0").show //per year per quater
  df.groupBy("brand").pivot("year").sum("profit").na.fill("0").show
  df.groupBy("brand","qtr").pivot("year").sum("profit").na.fill("0").show

  }
}
