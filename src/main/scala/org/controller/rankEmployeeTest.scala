package org.controller

import org.apache.spark.sql.expressions.Window
import org.util.{SparkOpener, readWriteUtil}
import org.constants.projectConstants
import org.apache.spark.sql.functions._

object rankEmployeeTest extends SparkOpener{

  def main(args:Array[String]):Unit={
    val spark=SparkSessionLoc("rank example")
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.basePathArgConstant,System.getProperty("user.dir")+"/Input/")
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"/Input/rankEmployee.txt")
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    val empDf=readWriteUtil.readDF(spark,inputMap)
    empDf.groupBy("emp_id","city").agg(count("city").as("city_count")).withColumn("rankedCol",rank() over(Window.orderBy(desc("city_count")))).filter("rankedCol=1").drop("rankedCol","city_count").orderBy("emp_id").show(false)
  }

}
