package org.controller.deltaLakeEG


import org.util.{SparkOpener, readWriteUtil}
import org.constants.projectConstants
import org.apache.spark.sql.DataFrame

object mergingSchemaNewColumnInBronze extends SparkOpener{
val spark=SparkSessionLoc("merge schema eg")
  def main(args: Array[String]): Unit = {
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
    {
      val argKey=arg.split("=",2)(0)
      val argValue=arg.split("=",2)(1)
      inputMap.put(argKey,argValue)
    }
    val modeForDeltaWrite=inputMap("mode")
    val outputBasePath=inputMap("basePath")+"outputFiles/"
    val inputBasePath=inputMap("basePath")+"inputFiles/"
    val deltaTableBaseName=inputMap("deltaTableBaseName")
    val deltaTableType=inputMap("deltaTableType")
    val selectExprNeeded=inputMap("selectExprNeeded").toLowerCase
    val mergeSchemaNeeded=inputMap("mergeSchemaNeeded").toLowerCase
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathArgConstant,inputBasePath)
    inputMap.put(projectConstants.filePathArgValue,inputBasePath+inputMap("fileName"))
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    var df:DataFrame=null

    selectExprNeeded match {
      case "yes" =>  df= readWriteUtil.readDF (spark, inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time","number_of_owners")
      case "no" => df= readWriteUtil.readDF(spark, inputMap)
      case _ => println("wrong selectExprNeeded selection")
    }
    df.printSchema
    mergeSchemaNeeded match {
      case "yes" =>    df.write.format (projectConstants.deltaFormat).option(projectConstants.deltaMergeSchemaClause,"true").mode(modeForDeltaWrite).partitionBy("brand", "model", "year", "month").save (outputBasePath + deltaTableBaseName + "_" + deltaTableType)
      case "no" => df.write.format(projectConstants.deltaFormat).mode(modeForDeltaWrite).partitionBy ("brand", "model", "year", "month").save (outputBasePath + deltaTableBaseName + "_" + deltaTableType)
      case _ => println("wrong mergeSchemaNeeded selection")
    }

    }

}
