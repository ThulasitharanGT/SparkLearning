package org.controller.ingestionAndDQC

import java.util.Calendar

import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DateType
import java.util.Date

object silverToGold extends sparkOpener {
def main(args:Array[String]):Unit = {
  // take count of messages per sessionType and circiuit and save as gold
  val inputMap = collection.mutable.Map[String, String]()
  for (arg <- args)
    inputMap.put(arg.split("=", 2)(0), arg.split("=", 2)(1))
  val spark = sparkSessionOpen()
  val processingDate = inputMap("processingDate")
  val calenderInstance = Calendar.getInstance()
  calenderInstance.setTime(dateFormat.parse(processingDate))
  calenderInstance.add(Calendar.DAY_OF_MONTH, -1)
  val dataDate = dateFormat.format(calenderInstance.getTime)
  val jobRunDate = dateFormat.format(new Date)
  val jobRunId = dateFormat.format(new Date)
  val logPath = s"${silverToGoldLogPath}${jobRunId}.log"
  var writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Gold Load job started for loading ${dataDate} in ${processingDate} \n")
  writerObject.writeBytes(s"Registering start entry in ${auditTable} table \n")
  writerObject.close
  inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameSilverToGold}',current_timestamp(),'${statusStarted}')") //'cast(${timeStampDateFormat.format(new Date)} as timestamp)'
  execSparkSql(spark, inputMap)
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Entered start entry in ${auditTable} table \n")
  writerObject.writeBytes(s"Reading from silver for date ${dataDate} table \n")
  writerObject.close
  try {
    inputMap.put(fileFormatArg, deltaFileFormat)
  inputMap.put(basePathArg, silverBasePath)
  inputMap.put(pathOption, s"${silverBasePath}processDate=${dataDate}") // date -1 data is processed in date
  val silverForCurrentDayDF = readDF(spark, inputMap)
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Read from silver for date ${dataDate} table \n")
  writerObject.close
  //Seq("key","topic","partition","offset","timestamp","timestampType","value[0] as reading","value[1] as year","value[2] as circuit","value[3] as session","value[4] as processDate")
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Computing gold aggregation \n")
  writerObject.close
  val goldAggregationCurrentDayDF = silverForCurrentDayDF.groupBy("year", "circuit", "session").agg(countDistinct("reading").as("distinctReadings")).withColumn("processDate", lit(dataDate).cast(DateType))
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Gold aggregation computed \n")
  writerObject.writeBytes(s"Computing gold ranks \n")
  writerObject.close
  val goldRankedComputedDF = silverForCurrentDayDF.withColumn("rnCol", dense_rank().over(Window.partitionBy("reading").orderBy(desc("offset")))).where("rnCol=1").drop("rnCol")
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Gold ranks computed \n")
  writerObject.close
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  inputMap.put(sysCommandArg, s"hdfs dfs -ls ${goldBasePath}processDate=${dataDate}")
  sysCommandExecuter(inputMap) match {
    case true => {
      writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
      writerObject.writeBytes(s"overwriting ${dataDate} date column in gold aggregation path. \n")
      writerObject.close
    }
    case false => {
      writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
      writerObject.writeBytes(s"writing ${dataDate} date column in gold aggregation path. \n")
      writerObject.close
    }
  }
  inputMap.put(pathOption, s"${goldBasePath}")
  inputMap.put(saveModeArg, overWriteMode)
  inputMap.put(partitionByFlag, stringTrue)
  inputMap.put(partitionByColumns, "processDate")
  writeDF(spark, inputMap, goldAggregationCurrentDayDF)
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Wrote ${dataDate} date column in gold aggregation. \n")
  writerObject.close
  inputMap.put(sysCommandArg, s"hdfs dfs -ls ${goldRankedBasePath}processDate=${dataDate}")
  sysCommandExecuter(inputMap) match {
    case true => {
      writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
      writerObject.writeBytes(s"Overwriting ${dataDate} date column in gold ranked path. \n")
      writerObject.close
    }
    case false => {
      writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
      writerObject.writeBytes(s"Writing ${dataDate} date column in gold ranked path. \n")
      writerObject.close
    }
  }
  inputMap.put(pathOption, s"${goldRankedBasePath}")
  writeDF(spark, inputMap, goldRankedComputedDF)
  writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
  writerObject.writeBytes(s"Wrote ${dataDate} date column in gold ranked path. \n")
    writerObject.writeBytes(s"Entering entry in ${auditTable} for completion \n")
    writerObject.close
    inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameSilverToGold}',current_timestamp(),'${statusFinished}')")//'cast(${timeStampDateFormat.format(new Date)} as timestamp)'
    execSparkSql(spark,inputMap)
    writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
    writerObject.writeBytes(s"Entered entry in ${auditTable} for completion \n")
    writerObject.close
}
  catch{
    case e:Exception => {
      writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
      writerObject.writeBytes(s"Entering entry in ${auditTable} for failure \n")
      writerObject.close
      inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameSilverToGold}',current_timestamp(),'${statusFailure}')")//'cast(${timeStampDateFormat.format(new Date)} as timestamp)'
      execSparkSql(spark,inputMap)
      writerObject = fileOutputStreamObjectCreator(hdfsDomainLocal, logPath)
      writerObject.writeBytes(s"Entered entry in ${auditTable} for failure \n")
      writerObject.writeBytes(s"Stack trace:\n ${e.printStackTrace} \n")
      writerObject.close
    }
  }
}
}
