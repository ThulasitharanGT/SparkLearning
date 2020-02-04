package org.controller.kafkaStreamExample

import org.constants.projectConstants
import org.util.{SparkOpener,readWriteUtil}
import org.apache.spark.sql.functions._

object streamAggregationStats extends SparkOpener{
val spark=SparkSessionLoc("raw table aggregation")
  def main(args: Array[String]): Unit = {
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valPart=arg.split("=",2)(1)
        inputMap.put(keyPart,valPart)
      }
    val streamInputPath=inputMap("streamInputPath")
    val statsOutputPath=inputMap("statsOutputPath")
    val checkPointLocation=inputMap("checkPointLocation")
    inputMap.put(projectConstants.fileFormatArg,projectConstants.deltaFormat)
    inputMap.put(projectConstants.filePathArgValue,streamInputPath)
    val bronzeSteamDF= readWriteUtil.readStreamFunction(spark,inputMap).groupBy("date","key").agg(count("value").as("totalMessages")) // spark.readStream.format(projectConstants.deltaFormat).load(steamInputPath)
    println("--------------------->stream read<------------------------")
    inputMap.put(projectConstants.pathArg,statsOutputPath)
    inputMap.put(projectConstants.checkPointLocationArg,checkPointLocation)
    inputMap.put(projectConstants.outputModeArg,"complete") //'append', 'complete', 'update'
    val aggOutputStreamDF=readWriteUtil.writeStreamFunction(spark,inputMap,bronzeSteamDF).partitionBy("date","key").option("spark.sql.sources.partitionOverwriteMode","dynamic")
    println("--------------------->stream write object created<------------------------")
    aggOutputStreamDF.start.awaitTermination
    //val aggOutputStreamDF= bronzeSteamDF.writeStream.outputMode("append").format(projectConstants.deltaFormat).option("checkpointLocation",checkPointLocation).option("path",statsOutputPath)

  }

}
