package org.controller

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.constants.projectConstants
import org.util.{SparkOpener, readWriteUtil}

import sys.process._
import scala.language.postfixOps

object partitionedTableRead extends SparkOpener{
  def main(args: Array[String]): Unit = {
    val inputMap=collection.mutable.Map[String,String]()
    val years="2011,2013,1998"
    val brands="Toyota,Ford,Hyundai"
    val basePath=System.getProperty("user.dir")
    val tableName="tablePartitioned"
    val tempPartitionedPath=basePath+projectConstants.windowsSep+tableName+projectConstants.windowsSep+"year=%year%"+projectConstants.windowsSep+"brand=%brand%"+projectConstants.windowsSep
    inputMap.put("years",years)
    inputMap.put("yearsReplacingParam","%year%")
    inputMap.put("brands",brands)
    inputMap.put("brandsReplacingParam","%brand%")
    inputMap.put("years",years)
    inputMap.put("yearSeparator",projectConstants.delimiterComma)
    inputMap.put("brandSeparator",projectConstants.delimiterComma)
    inputMap.put("tempBasePath",tempPartitionedPath)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeParquetValue)
    val spark=SparkSessionLoc("temp")
    reqCarDetailsLoad(spark,inputMap)
  }
  def reqCarDetailsLoad(spark:SparkSession,inputMap: collection.mutable.Map[String,String])= {
    var df: DataFrame = null
    val yearList = inputMap("years").split(inputMap("yearSeparator")).toList
    val brandList = inputMap("brands").split(inputMap("brandSeparator")).toList
    val brandReplaceParameter = inputMap("brandsReplacingParam")
    val yearReplaceParameter = inputMap("yearsReplacingParam")
    val basepath = inputMap("tempBasePath")
    for (year <- yearList)
      for (brand <- brandList) {
        inputMap.put(projectConstants.filePathArgValue, basepath.replace(yearReplaceParameter, year).replace(brandReplaceParameter, brand))
        inputMap.put(projectConstants.fileFormatArg, projectConstants.fileTypeParquetValue)
        // Dynamicaly reading if exists
        println(inputMap(projectConstants.filePathArgValue))
        val result = ("ls "+inputMap(projectConstants.filePathArgValue) !).toInt
          result match {
          case 0 => {
            if (df == null)
              df = readWriteUtil.readDF(spark, inputMap)
            else
              df.union(readWriteUtil.readDF(spark, inputMap))
          }
          case 1 => print(inputMap(projectConstants.filePathArgValue) + "Does not exist !")
          case _ => df
        }

      }
    df
  }

}
