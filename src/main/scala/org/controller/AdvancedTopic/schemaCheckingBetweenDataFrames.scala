package org.controller.AdvancedTopic

import org.apache.spark.sql.DataFrame
import org.constants.projectConstants
import org.util.{SparkOpener, readWriteUtil}

object schemaCheckingBetweenDataFrames extends SparkOpener{
  val spark=SparkSessionLoc("schemaChecker")
  def main (args:Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for(arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
    val srcDFPath=inputMap("srcDFPath")
    val destDFPath=inputMap("destDFPath")
    val srcDFBasePath=inputMap("srcDFBasePath")
    val destDFBasePath=inputMap("destDFBasePath")

    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathArgConstant,srcDFBasePath)
    inputMap.put(projectConstants.pathArg,srcDFPath)
    val srcDF=readWriteUtil.readDF(spark,inputMap)
    inputMap.put(projectConstants.basePathArgConstant,destDFBasePath)
    inputMap.put(projectConstants.pathArg,destDFPath)
    val destDF=readWriteUtil.readDF(spark,inputMap)
    dfSchemaChecker(srcDF,destDF)
  }

  def dfSchemaChecker(dfSrc:DataFrame,dfDest:DataFrame)=
  {
    val dfSchemaSrc=dfSrc.dtypes.map(x  =>x.toString).map(x=>x.toString.substring(1,x.toString.length-1))
    val dfSchemaDest=dfDest.dtypes.map(x  =>x.toString).map(x=>x.toString.substring(1,x.toString.length-1))

    val srcSchemaMap:collection.mutable.LinkedHashMap[String,String]= collection.mutable.LinkedHashMap[String,String]()
    val destSchemaMap:collection.mutable.LinkedHashMap[String,String]= collection.mutable.LinkedHashMap[String,String]()

    for(dfTempSchemaSrc <- dfSchemaSrc)
    srcSchemaMap.put(dfTempSchemaSrc.split(",",2)(0),dfTempSchemaSrc.split(",",2)(1)) //column,dtype
    for(dfTempSchemaDest <- dfSchemaDest)
    destSchemaMap.put(dfTempSchemaDest.split(",",2)(0),dfTempSchemaDest.split(",",2)(1)) //column,dtype



    val colNamesSrc=srcSchemaMap.keys.toArray
    val colNamesDest=destSchemaMap.keys.toArray
    if (colNamesDest.size == colNamesSrc.size)
    {
      var resultColCount:Int=0
      val numOfColumns=colNamesDest.size // colNamesSrc.size
      for (i <- 0 to numOfColumns -1)
      {
        if (colNamesSrc(i).toLowerCase == colNamesDest(i).toLowerCase) // col name match
          if(srcSchemaMap(colNamesSrc(i)).toLowerCase == destSchemaMap(colNamesDest(i)).toLowerCase )  // schema match
            resultColCount=resultColCount+1
      }
      resultColCount match {case value if value == numOfColumns => println("Schema matched") case _ => println("Schema did not match") }
    }
    else
      println("Number of colum's did not match")
  }
//  dfSchemaChecker(srcDF,destDF)
}
