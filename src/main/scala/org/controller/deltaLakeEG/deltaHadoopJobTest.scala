package org.controller.deltaLakeEG

import org.apache.spark.sql.DataFrame
import org.util.{SparkOpener, readWriteUtil}
import org.constants.projectConstants
import io.delta.tables.DeltaTable._
// for bronze
object deltaHadoopJobTest extends SparkOpener{

  val spark=SparkSessionLoc("DeltaHadoopJob")
  def main (args : Array[String]):Unit ={
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
      {
        val argKey=arg.split("=",2)(0)
        val argValue=arg.split("=",2)(1)
        inputMap.put(argKey,argValue)
      }
    val modeForDeltaWrite=inputMap("mode")
    val createOrAppendForDeltaWrite=inputMap("createOrAppendForDeltaWrite")
    val outputBasePath=inputMap("basePath")+"outputFiles/"
    val inputBasePath=inputMap("basePath")+"inputFiles/"
    val deltaTableBaseName=inputMap("deltaTableBaseName")
    val deltaTableType=inputMap("deltaTableType")
    val selectExprNeeded=inputMap("selectExprNeeded").toLowerCase

    println("------------------------------Param----------------------------" )
    println("modeForDeltaWrite === "+modeForDeltaWrite)
    println("createOrAppendForDeltaWrite === "+createOrAppendForDeltaWrite)
    println("outputBasePath === "+outputBasePath)
    println("inputBasePath === "+inputBasePath)
    println("deltaTableBaseName === "+deltaTableBaseName)
    println("deltaTableType === "+deltaTableType)
    println("selectExprNeeded === "+selectExprNeeded)

    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathArgConstant,inputBasePath)
    inputMap.put(projectConstants.filePathArgValue,inputBasePath+inputMap("fileName"))
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    var df:DataFrame=null
    selectExprNeeded match {
      case "yes" =>  df= readWriteUtil.readDF(spark, inputMap).selectExpr("Vehicle_id", "model", "brand", "year", "month", "miles", "CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
      case "no"=> df= readWriteUtil.readDF(spark, inputMap)
      case _ => println("wrong selectExprNeeded selection")
    }

    createOrAppendForDeltaWrite match {
      case "create" => { // as partitioning is not working in repl go with the creation of table as arquet and converting the parq using convert to delta  class
        df.write.mode(modeForDeltaWrite).partitionBy("brand","model","year","month").save(outputBasePath+deltaTableBaseName+"_"+deltaTableType)
        convertToDelta(spark,"parquet.`"+outputBasePath+deltaTableBaseName+"_"+deltaTableType+"`","brand String, model String, year int, month int") //Pre defined function
      }
      case "append" => df.write.format(projectConstants.deltaFormat).mode(modeForDeltaWrite).partitionBy("brand","model","year","month").save(outputBasePath+deltaTableBaseName+"_"+deltaTableType)
      case _ => println("wrong createOrAppendForDeltaWrite selection")

    }
/*
//creating delta table . for bronze table

cd /home/raptor/IdeaProjects/SparkLearning/build/libs

spark-submit --class org.controller.deltaLakeEG.deltaHadoopJobTest --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar mode=append createOrAppendForDeltaWrite=create basePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/ deltaTableBaseName=carDetailTable deltaTableType=Bronze selectExprNeeded=Yes fileName=Avail_car3.txt


appending delta table
cd /home/raptor/IdeaProjects/SparkLearning/build/libs

spark-submit --class org.controller.deltaLakeEG.deltaHadoopJobTest --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar mode=append createOrAppendForDeltaWrite=append basePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/ deltaTableBaseName=carDetailTable deltaTableType=Bronze selectExprNeeded=Yes fileName=Avail_car4.txt



appending delta table  wit no select EXPR - File specific
cd /home/raptor/IdeaProjects/SparkLearning/build/libs

spark-submit --class org.controller.deltaLakeEG.deltaHadoopJobTest --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1  --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar mode=append createOrAppendForDeltaWrite=append basePath=hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/ deltaTableBaseName=carDetailTable deltaTableType=Bronze selectExprNeeded=No fileName=Avail_car2.txt

 */
  }

}
