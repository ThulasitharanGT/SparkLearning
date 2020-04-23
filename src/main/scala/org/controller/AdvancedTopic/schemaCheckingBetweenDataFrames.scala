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
    inputMap.put(projectConstants.filePathArgValue,srcDFPath)
    val srcDF=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
    // schema wont match if select expr is commented above as it's invalid date format and will be considered a string
    inputMap.put(projectConstants.basePathArgConstant,destDFBasePath)
    inputMap.put(projectConstants.filePathArgValue,destDFPath)
    val destDF=readWriteUtil.readDF(spark,inputMap)
    dfSchemaChecker(srcDF,destDF)
  }

  def dfSchemaChecker(dfSrc:DataFrame,dfDest:DataFrame)=
  {
    val dfSchemaSrc=dfSrc.dtypes.map(x  =>x.toString).map(x=> x.substring(1,x.size-1))
    val dfSchemaDest=dfDest.dtypes.map(x  =>x.toString).map(x=> x.substring(1,x.size-1))

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
      println("Number of column's did not match")
  }
//  dfSchemaChecker(srcDF,destDF)
  //spark-submit --class org.controller.AdvancedTopic.schemaCheckingBetweenDataFrames --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g SparkLearning-1.0-SNAPSHOT.jar  srcDFPath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car3.txt destDFPath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car2.txt srcDFBasePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/ destDFBasePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/
/*

val srcDF=spark.read.format("com.databricks.spark.csv").option("delimiter","|").option("header","true").option("inferSchema","true").load("/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car3.txt").selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")

val destDF=spark.read.format("com.databricks.spark.csv").option("delimiter","|").option("header","true").option("inferSchema","true").load("/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car2.txt")


 */


}
