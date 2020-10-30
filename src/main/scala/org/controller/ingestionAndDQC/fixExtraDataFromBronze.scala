package org.controller.ingestionAndDQC

import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._


import java.util.Date
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.util.control.Breaks._

object fixExtraDataFromBronze extends sparkOpener {
  def main(args: Array[String]): Unit = {
    val inputMap = collection.mutable.Map[String, String]()
    for (arg <- args)
      inputMap.put(arg.split("=", 2)(0), arg.split("=", 2)(1))
    val spark = sparkSessionOpen()
    spark.sparkContext.setLogLevel("ERROR")
    val inputDateFromStatsTable = inputMap("inputDateFromStatsTable")
    val jobRunId = jobRunIDDateFormat.format(new Date)
    val jobRunDate = dateFormat.format(new Date)
    val logFile=s"${fixExtraBronzeDataToSilverLogPath}${jobRunId}.log"
    var writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
    writerObject.writeBytes(s"Extra data copy job started based on latest stats from ${statsTable} in date ${inputDateFromStatsTable}\n")
    writerObject.writeBytes(s"Reading the latest entered JobRunID details from ${statsTable}\n")
    writerObject.close
    inputMap.put(sqlStringArg, s"select job_run_id,data_date,difference_prev_day_bronze_vs_curr_day_bronze from ${statsTable} where job_run_date=to_date('${inputDateFromStatsTable}')")
    val inputStatsDataForCurrentJobDF = execSparkSql(spark, inputMap).withColumn("rankCol", dense_rank.over(Window.orderBy(desc("job_run_id")))).filter("rankCol=1").drop("rankCol")
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
    writerObject.writeBytes(s"Read the latest entered JobRunID details from ${statsTable}\n")
    writerObject.close
    val datesToBeReloaded = inputStatsDataForCurrentJobDF.filter("difference_prev_day_bronze_vs_curr_day_bronze != 0").select("data_date").collect.map(_ (0).toString).toList
    val failedJobJobId = inputStatsDataForCurrentJobDF.select("job_run_id").distinct.collect.map(_ (0).toString).toList(0)
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
    writerObject.writeBytes(s"The latest entered JobRunID is ${failedJobJobId}\n")
    writerObject.writeBytes(s"Inserting start status in ${auditTable} with Job ID \n")
    writerObject.close
    inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameFixExtraRecordsInBronzeIntoSilver} - for ${failedJobJobId}',current_timestamp(),'${statusStarted}')") //'${timeStampDateFormat.format(new Date)}'
    execSparkSql(spark, inputMap)
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
    writerObject.writeBytes(s"Start status inserted in ${auditTable}\n")
    writerObject.close
    try {
      for (dateToBeLoaded <- datesToBeReloaded) {
        breakable {
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Entering start status entry for ${dateToBeLoaded} \n")
          writerObject.close
          inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameFixExtraRecordsInBronzeIntoSilver} - for ${dateToBeLoaded}',current_timestamp(),'${statusStarted}')") //'${timeStampDateFormat.format(new Date)}'
          execSparkSql(spark, inputMap)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Entered start status entry for ${dateToBeLoaded}\n")
          writerObject.close
          inputMap.put(fileFormatArg, deltaFileFormat)
          inputMap.put(basePathArg, s"${bronzeBasePath}")
          inputMap.put(pathOption, s"${bronzeBasePath}processDate=${dateToBeLoaded}")
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Reading bronze records for ${dateToBeLoaded} \n")
          writerObject.close
          val bronzeDataForCurrentFixDateDF = readDF(spark, inputMap)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Read all bronze records for ${dateToBeLoaded} \n")
          writerObject.close
          inputMap.put(basePathArg, s"${silverBasePath}")
          inputMap.put(pathOption, s"${silverBasePath}processDate=${dateToBeLoaded}")
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Reading silver records for ${dateToBeLoaded} \n")
          writerObject.close
          val silverDataForCurrentFixDateDF = readDF(spark, inputMap)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Read all silver records for ${dateToBeLoaded} \n")
          writerObject.writeBytes(s"Finding later arrived records in bronze for ${dateToBeLoaded} \n")
          writerObject.close
          val extraRecordsDF = bronzeDataForCurrentFixDateDF.as("bronzeDataSet").join(silverDataForCurrentFixDateDF.as("silverDataSet"), col("bronzeDataSet.key") === col("silverDataSet.key") && col("bronzeDataSet.topic") === col("silverDataSet.topic") && col("bronzeDataSet.partition") === col("silverDataSet.partition") && col("bronzeDataSet.offset") === col("silverDataSet.offset") && col("bronzeDataSet.timestamp") === col("silverDataSet.timestamp") && col("bronzeDataSet.timestampType") === col("silverDataSet.timestampType") && col("bronzeDataSet.reading") === col("silverDataSet.reading") && col("bronzeDataSet.year") === col("silverDataSet.year") && col("bronzeDataSet.circuit") === col("silverDataSet.circuit") && col("bronzeDataSet.session") === col("silverDataSet.session") && col("bronzeDataSet.processDate") === col("silverDataSet.processDate"), "full_outer").where("silverDataSet.key is null").selectExpr("bronzeDataSet.*")
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Found all late records in bronze for ${dateToBeLoaded}\n")
          writerObject.writeBytes(s"Saving the late records in silver for ${dateToBeLoaded} to a temporary path.\n")
          writerObject.close
          inputMap.put(pathOption, s"${extraRecordsBronzeCatchupTempPathForCompaction}")
          inputMap.put(saveModeArg, overWriteMode)
          writeDF(spark, inputMap, extraRecordsDF)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Saved the late records in silver for ${dateToBeLoaded} to a temporary path.Count of records that came in late is ${extraRecordsDF.count} .\n")
          writerObject.writeBytes(s"Performing compaction.\n")
          writerObject.close
          val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
          val dirSize = fs.getContentSummary(new Path(s"${extraRecordsBronzeCatchupTempPathForCompaction}")).getLength // in bytes
          val numOfFiles = Math.ceil(dirSize / (1024 * 1024)) match {
            case value if value != 0.asInstanceOf[Long] => value.toInt
            case _ => 1.toInt
          }
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Performed compaction, ${numOfFiles} number of files will be created.\n ")
          writerObject.close
          inputMap.put(pathOption, s"${silverBasePath}")
          inputMap.put(partitionByFlag, stringTrue)
          inputMap.put(partitionByColumns, "processDate")
          inputMap.put(DFrepartitionArg, "2")
          inputMap.put(coalesceArg, s"${numOfFiles}")
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Writing extra records to orginal silver path.\n ")
          writerObject.close
          try {
            writeDF(spark, inputMap, extraRecordsDF)
          }
          catch {
            case e: Exception => {
              writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
              writerObject.writeBytes(s"Error in writing to silver for Date ${dateToBeLoaded} for stats failure job ID ${failedJobJobId}\nStack Trace :\n${e.printStackTrace}\n")
              writerObject.writeBytes(s"Inserting failure status into audit table.\n")
              writerObject.close
              println(s"Error in writing to silver for Date ${dateToBeLoaded} for stats failure job ID ${failedJobJobId}\nStack Trace :\n${e.printStackTrace}\n")
              inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameFixExtraRecordsInBronzeIntoSilver} - for ${dateToBeLoaded}',current_timestamp(),'${statusFailure}')") //'${timeStampDateFormat.format(new Date)}'
              execSparkSql(spark, inputMap)
              writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
              writerObject.writeBytes(s"Inserted failure status into audit table.\n")
              writerObject.writeBytes(s"Breaking for current fix date and running for next fix date if it exists.\n")
              writerObject.close
              break
            }
          }
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Wrote extra records to orginal silver path.\n ")
          writerObject.writeBytes(s"Entering finish status entry for ${dateToBeLoaded} in ${auditTable} \n")
          writerObject.close
          inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameFixExtraRecordsInBronzeIntoSilver} - for ${dateToBeLoaded}',current_timestamp(),'${statusSuccess}')") //'${timeStampDateFormat.format(new Date)}'
          execSparkSql(spark, inputMap)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Entered finish status entry for ${dateToBeLoaded} in ${auditTable} \n")
          writerObject.close
        }
      }
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
      writerObject.writeBytes(s"Wrote extra records to orginal silver path.\n ")
      writerObject.writeBytes(s"Entering finish status entry for data fix job with failed stats job jobId ${failedJobJobId} \n")
      writerObject.close
      inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameFixExtraRecordsInBronzeIntoSilver} - for ${failedJobJobId}',current_timestamp(),'${statusSuccess}')") //'${timeStampDateFormat.format(new Date)}'
      execSparkSql(spark, inputMap)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
      writerObject.writeBytes(s"Entered finish status entry for data fix job with failed stats job jobId ${failedJobJobId} \n")
      writerObject.close
    }
    catch
      {
        case e:Exception => {
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Error in fixing data to silver for stats failure job ID ${failedJobJobId}\nStack Trace :\n${e.printStackTrace}\n")
          writerObject.writeBytes("Inserting failure status into audit table.\n")
          writerObject.close
          println(s"Error in fixing data to silver for stats failure job ID ${failedJobJobId}\nStack Trace :\n${e.printStackTrace}")
          inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameFixExtraRecordsInBronzeIntoSilver} - for ${failedJobJobId}',current_timestamp(),'${statusFailure}')") //'${timeStampDateFormat.format(new Date)}'
          execSparkSql(spark, inputMap)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
          writerObject.writeBytes(s"Inserted failure status into audit table.\n")
          writerObject.close
        }
      }
  }
}
