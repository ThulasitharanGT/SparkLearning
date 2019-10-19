package org.controller

import org.util.{SparkOpener,readWriteUtil}


import org.constants.projectConstants

object PivotExample extends SparkOpener {
  def main(args: Array[String]): Unit ={

    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathValueConstant,System.getProperty("user.dir")+"\\Input")
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"\\Input\\Car_pivot_example.txt")
  val spark = SparkSessionLoc("SparkSession")
  val df=readWriteUtil.readDF(spark,inputMap)
  df.groupBy("brand","year").pivot("qtr").sum("profit").na.fill("0").show //per year per quater
  df.groupBy("brand").pivot("year").sum("profit").na.fill("0").show
  df.groupBy("brand","qtr").pivot("year").sum("profit").na.fill("0").show

    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringFalse)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvHeaderColumnPassedValue)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathValueConstant,System.getProperty("user.dir")+"\\Input")
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"\\Input\\carPivotWithoutHeader.txt")
    inputMap.put(projectConstants.columnNameArg,"Vehicle_id,model,brand,year,qtr,Profit")
    inputMap.put(projectConstants.columnNameSepArg,projectConstants.delimiterComma)
    val newDf=readWriteUtil.readDF(spark,inputMap)
    newDf.show
  }
}
