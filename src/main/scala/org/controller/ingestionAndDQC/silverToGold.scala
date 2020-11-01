package org.controller.ingestionAndDQC

import java.util.Calendar

import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType


object silverToGold extends sparkOpener {
def main(args:Array[String]):Unit ={
  // take count of messages per sessionType and circiuit and save as gold
  val inputMap=collection.mutable.Map[String,String]()
  for (arg <- args)
    inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
  val spark=sparkSessionOpen()
  val processingDate=inputMap("processingDate")
  val calenderInstance = Calendar.getInstance()
  calenderInstance.setTime(dateFormat.parse(processingDate))
  calenderInstance.add(Calendar.DAY_OF_MONTH,-1)
  val dataDate=dateFormat.format(calenderInstance.getTime)
  inputMap.put(fileFormatArg,deltaFileFormat)
  inputMap.put(basePathArg,silverBasePath)
  inputMap.put(pathOption,s"${silverBasePath}processDate=${dataDate}")  // date -1 data is processed in date
  val silverForCurrentDayDF=readDF(spark,inputMap)
  //Seq("key","topic","partition","offset","timestamp","timestampType","value[0] as reading","value[1] as year","value[2] as circuit","value[3] as session","value[4] as processDate")
  val goldAggregationCurrentDayDF=silverForCurrentDayDF.groupBy("year","circuit","session").agg(countDistinct("reading").as("distinctReadings")).withColumn("processDate",lit(dataDate).cast(DateType))
  val goldRankedComputedDF=silverForCurrentDayDF.withColumn("rnCol",dense_rank().over(Window.partitionBy("reading").orderBy(desc("offset")))).where("rnCol=1").drop("rnCol")
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  inputMap.put(pathOption,s"${goldBasePath}")
  inputMap.put(saveModeArg,overWriteMode)
  inputMap.put(partitionByFlag,stringTrue)
  inputMap.put(partitionByColumns,"processDate")
  writeDF(spark,inputMap,goldAggregationCurrentDayDF)
  inputMap.put(pathOption,s"${goldRankedBasePath}")
  writeDF(spark,inputMap,goldRankedComputedDF)
  }
}
