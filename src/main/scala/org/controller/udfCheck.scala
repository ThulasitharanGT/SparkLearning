package org.controller

import java.util.Calendar

import org.util.SparkOpener

object udfCheck extends SparkOpener {
  val curYear = Calendar.getInstance().get(Calendar.YEAR)
  val curMonth = Calendar.getInstance().get(Calendar.MONTH)


  def main(args: Array[String]): Unit = {
    val spark=SparkSessionLoc("check udf")
    spark.udf.register("maxBuyPercntage",percentageMaxBuyPrice(_:Int,_:Int,_:Int))
    val availDf=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load(System.getProperty("user.dir")+"\\Input\\availCarForUdf.txt")
    val newDf=availDf.selectExpr("Vehicle_id","model","brand","year","month","miles","maxBuyPercntage(year,month,miles) as maxBuyPercntage")
    newDf.filter("maxBuyPercntage> 50").show
  }
  def percentageMaxBuyPrice(year:Int,month:Int,miles:Int) ={
  val yearDiff=curYear-year
  val monthDiff= (13-month)+(yearDiff-1)*12+curMonth
  var percentage=0
  yearDiff match
    {
    case 0 => monthDiff match {case value if value<=6 => percentage=90 case value if value>6 && value <=9 => percentage=80  case _ => percentage=75}
    case 1 => monthDiff match {case value if value<=6 => percentage=75  case _ => percentage=70}
    case 2 => monthDiff match {case value if value<=6 => percentage=65  case _ => percentage=60}
    case 3 => monthDiff match {case value if value<=6 => percentage=55  case _ => percentage=50}
    case _ => percentage=50
  }
    percentage
  }
}
