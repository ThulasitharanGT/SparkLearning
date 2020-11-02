package org.controller.reRunnableJob

import java.text.SimpleDateFormat
import java.util.Date
import  org.controller.reRunnableJob.constants._
import  org.controller.reRunnableJob.jobFunctions._
import  org.controller.reRunnableJob.readWriteUtil._
// this job uses one audit table to check the last run subjob and it's stats . If its a failure then we need to restart from there, else it will start new
/*
audit table ddl
create database tempDb;
create table tempDb.temp_audit_table
(
job_name string,
sub_job_name string,
job_creation_date timestamp,
job_updation_date timestamp,
run_date string,
job_status string,
job_run_id bigint
)
stored as parquet
location
'/user/raptor/hadoop/reRunnableJob/tempDb/external/temp_audit_table/';

CREATE TABLE tempdb.temp_pipeline_archive(
element string,
number int,
symbol string,
weight double,
boil double,
melt double,
density_vapour double,
fusion double,
diffusion_index double)
STORED AS PARQUET
LOCATION
'hdfs:///user/raptor/hadoop/reRunnableJob/tempDb/external/temp_pipeline_archive/';

CREATE TABLE tempdb.pipeline_temp_1(
element string,
number int,
symbol string,
weight double,
boil double,
melt double,
density_vapour double,
fusion double,
diffusion_index double,
datadate string)
STORED AS PARQUET
LOCATION
'hdfs:///user/raptor/hadoop/reRunnableJob/tempDb/external/pipeline_temp_1/';


CREATE TABLE tempdb.pipeline_temp_2(
element string,
number int,
symbol string,
weight double,
boil double,
melt double,
density_vapour double,
fusion double,
diffusion_index double,
datadate string,
jobrunid string)
STORED AS PARQUET
LOCATION
'hdfs:///user/raptor/hadoop/reRunnableJob/tempDb/external/pipeline_temp_2/';
 */

object reRunnableMain extends sparkOpener{
def main(args:Array[String]):Unit= {
  val spark = sparkSessionOpen("re runnable Main")
  spark.sparkContext.setLogLevel("ERROR")
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
  val dateFormatterJobId = new SimpleDateFormat("yyyyMMddhhmmss")
  val inputMap = collection.mutable.Map[String, String]()
  for (arg <- args)
    inputMap.put(arg.split("=")(0), arg.split("=")(1))
  val date = new Date()
  //val runDate=dateFormatter.format(date)
  val runDate = inputMap("runDate")
  val pwd = inputMap("pwd")
  val jobRunID = dateFormatterJobId.format(date)
  //val runDate="2020-05-15"
  /*val redshiftUrl = ""//jdbc url
    val redshiftUser = "" //jdbc user name
    val redshiftPwd = "" // jdbc password
    val redshiftDriver = "com.amazon.redshift.jdbc42.Driver"*/
  val mainJobName = "temp_load"
  val logPathArg = "logPath"
  val hdfsDomain = "hdfs.domain"

  val logPath = s"hdfs:///user/raptor/hadoop/logs/${dateFormatterJobId.format(date)}.log"
  val hdfsCurrentDomain = "hdfs://localhost:8020/"
  inputMap.put(logPathArg, logPath)
  inputMap.put(hdfsDomain, hdfsCurrentDomain)
  inputMap.put(mainJobNameArg, mainJobName)
  inputMap.put(runDateArg, runDate)
  inputMap.put(jobRunIdArg, jobRunID)
  inputMap.put(sqlStringArg, s"select * from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}')")
  val dfCount = execSparkSql(spark, inputMap).count
  var fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
  fileOutputStream.writeBytes(s"Records found for ${runDate} in Audit table is ${dfCount} \n")
  fileOutputStream.close
  println(dfCount)

    try {
    dfCount match {
      case value if value == 0.asInstanceOf[Long] => {
        /*inputMap.put(userOption, redshiftUser)
          inputMap.put(urlOption, redshiftUrl)
          inputMap.put(passwordOption, redshiftPwd)
          inputMap.put(driverOption, redshiftDriver)
          inputMap.put(fileFormatArg, jdbcFormat)*/
        // overload
        inputMap.put(fileFormatArg, csvFileFormatArg)
        inputMap.put(headerOption, stringTrue)
        inputMap.put(delimiterOption, delimiterComma)
        inputMap.put(inferSchemaOption, stringTrue)
        inputMap.put(currentJobNameArg, subJobNames(0))
        inputMap.put(wgetFileNameArg, s"/home/raptor/temp/fileDownload/data_${inputMap(jobRunIdArg)}.csv")
        inputMap.put(wgetHttpPathArg, s"https://introcs.cs.princeton.edu/java/data/elements.csv")
        println("one (read) from redshift")
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Starting one (read) from redshift\n")
        fileOutputStream.close
        // val redshiftDF=loadFromRedshift(spark,inputMap)
        val redshiftDF = loadFromHttp(spark, inputMap)
        println("1.5 Archival ")
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Completed one (read) from redshift\n")
        fileOutputStream.close
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Starting 1.5 Archival")
        fileOutputStream.close
        inputMap.put(currentJobNameArg, subJobNames(1))
        archivalForRedshift(redshiftDF, spark, inputMap)
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Completed 1.5 Archival\n")
        fileOutputStream.close
        println("second (insert) ")
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Starting second (insert)\n")
        fileOutputStream.close
        inputMap.put(currentJobNameArg, subJobNames(2))
        inputMap.put(DFrepartitionArg, "2")
        inputMap.put(saveModeArg, "append")
        inputMap.put(tableNameArg, "tempDb.pipeline_temp_1")
        insertDeltaToHive(redshiftDF, spark, inputMap)
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Completed second (insert)\n")
        fileOutputStream.close
        println("third (archival)")
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Starting third (archival)\n")
        fileOutputStream.close
        inputMap.put(currentJobNameArg, subJobNames(3))
        snapShotHive(redshiftDF, spark, inputMap)
        fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
        fileOutputStream.writeBytes("Completed third (archival)\n")
        fileOutputStream.close
      }
      case value if value > 0.asInstanceOf[Long] => {
        inputMap.put(sqlStringArg, s"select * from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}') order by job_updation_date desc limit 1")
        val dfStatus = execSparkSql(spark, inputMap)
        // dfStatus.show(false)
        dfStatus.select("sub_job_name").filter(s"job_status ='${failedString}'").count match {
          case value if value == 0.asInstanceOf[Long] => {
            /*inputMap.put(userOption, redshiftUser)
         inputMap.put(urlOption, redshiftUrl)
         inputMap.put(passwordOption, redshiftPwd)
         inputMap.put(driverOption, redshiftDriver)
         inputMap.put(fileFormatArg, jdbcFormat)*/
            // overload
            inputMap.put(fileFormatArg, csvFileFormatArg)
            inputMap.put(headerOption, stringTrue)
            inputMap.put(delimiterOption, delimiterComma)
            inputMap.put(inferSchemaOption, stringTrue)
            inputMap.put(currentJobNameArg, subJobNames(0))
            inputMap.put(wgetFileNameArg, s"/home/raptor/temp/fileDownload/data_${inputMap(jobRunIdArg)}.csv")
            inputMap.put(wgetHttpPathArg, s"https://introcs.cs.princeton.edu/java/data/elements.csv")
            println("one (read) from redshift")
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Starting one (read) from redshift\n")
            fileOutputStream.close
            // val redshiftDF=loadFromRedshift(spark,inputMap)
            val redshiftDF = loadFromHttp(spark, inputMap)
            println("1.5 Archival ")
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Completed one (read) from redshift\n")
            fileOutputStream.close
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Starting 1.5 Archival")
            fileOutputStream.close
            inputMap.put(currentJobNameArg, subJobNames(1))
            archivalForRedshift(redshiftDF, spark, inputMap)
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Completed 1.5 Archival\n")
            fileOutputStream.close
            println("second (insert) ")
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Starting second (insert)\n")
            fileOutputStream.close
            inputMap.put(currentJobNameArg, subJobNames(2))
            inputMap.put(DFrepartitionArg, "2")
            inputMap.put(saveModeArg, "append")
            inputMap.put(tableNameArg, "tempDb.pipeline_temp_1")
            insertDeltaToHive(redshiftDF, spark, inputMap)
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Completed second (insert)\n")
            fileOutputStream.close
            println("third (archival)")
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Starting third (archival)\n")
            fileOutputStream.close
            inputMap.put(currentJobNameArg, subJobNames(3))
            snapShotHive(redshiftDF, spark, inputMap)
            fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
            fileOutputStream.writeBytes("Completed third (archival)\n")
            fileOutputStream.close
          }
          case _ => {
            val failedJobName = dfStatus.select("sub_job_name").filter(s"job_status ='${failedString}'").collect.map(_ (0).asInstanceOf[String]).toList(0)
            failedJobName match {
              case value if value == subJobNames(0) => {
                /*inputMap.put(userOption, redshiftUser)
        inputMap.put(urlOption, redshiftUrl)
        inputMap.put(passwordOption, redshiftPwd)
        inputMap.put(driverOption, redshiftDriver)
        inputMap.put(fileFormatArg, jdbcFormat)*/
                // overload
                inputMap.put(fileFormatArg, csvFileFormatArg)
                inputMap.put(headerOption, stringTrue)
                inputMap.put(delimiterOption, delimiterComma)
                inputMap.put(inferSchemaOption, stringTrue)
                inputMap.put(currentJobNameArg, subJobNames(0))
                inputMap.put(wgetFileNameArg, s"/home/raptor/temp/fileDownload/data_${inputMap(jobRunIdArg)}.csv")
                inputMap.put(wgetHttpPathArg, s"https://introcs.cs.princeton.edu/java/data/elements.csv")
                println("one (read) from redshift")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting one (read) from redshift\n")
                fileOutputStream.close
                // val redshiftDF=loadFromRedshift(spark,inputMap)
                val redshiftDF = loadFromHttp(spark, inputMap) // we don't have redshift so we wget and run the data
                println("1.5 Archival ")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed one (read) from redshift\n")
                fileOutputStream.close
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting 1.5 Archival")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(1))
                archivalForRedshift(redshiftDF, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed 1.5 Archival\n")
                fileOutputStream.close
                println("second (insert) ")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting second (insert)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(2))
                inputMap.put(DFrepartitionArg, "2")
                inputMap.put(saveModeArg, "append")
                inputMap.put(tableNameArg, "tempDb.pipeline_temp_1")
                insertDeltaToHive(redshiftDF, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed second (insert)\n")
                fileOutputStream.close
                println("third (archival)")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting third (archival)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(3))
                snapShotHive(redshiftDF, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed third (archival)\n")
                fileOutputStream.close
              }
              case value if value == subJobNames(1) => {
                /*inputMap.put(userOption, redshiftUser)
        inputMap.put(urlOption, redshiftUrl)
        inputMap.put(passwordOption, redshiftPwd)
        inputMap.put(driverOption, redshiftDriver)
        inputMap.put(fileFormatArg, jdbcFormat)*/
                // overload
                inputMap.put(fileFormatArg, csvFileFormatArg)
                inputMap.put(headerOption, stringTrue)
                inputMap.put(delimiterOption, delimiterComma)
                inputMap.put(inferSchemaOption, stringTrue)
                inputMap.put(currentJobNameArg, subJobNames(0))
                inputMap.put(wgetFileNameArg, s"/home/raptor/temp/fileDownload/data_${inputMap(jobRunIdArg)}.csv")
                inputMap.put(wgetHttpPathArg, s"https://introcs.cs.princeton.edu/java/data/elements.csv")
                println("one (read) from redshift")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting one (read) from redshift\n")
                fileOutputStream.close
                // val redshiftDF=loadFromRedshift(spark,inputMap)
                val redshiftDF = loadFromHttp(spark, inputMap)
                println("1.5 Archival ")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed one (read) from redshift\n")
                fileOutputStream.close
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting 1.5 Archival")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(1))
                archivalForRedshift(redshiftDF, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed 1.5 Archival\n")
                fileOutputStream.close
                println("second (insert) ")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting second (insert)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(2))
                inputMap.put(DFrepartitionArg, "2")
                inputMap.put(saveModeArg, "append")
                inputMap.put(tableNameArg, "tempDb.pipeline_temp_1")
                insertDeltaToHive(redshiftDF, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed second (insert)\n")
                fileOutputStream.close
                println("third (archival)")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting third (archival)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(3))
                snapShotHive(redshiftDF, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed third (archival)\n")
                fileOutputStream.close
              }
              case value if value == subJobNames(2) => {
                inputMap.put(sqlStringArg, s"select * from tempDb.temp_pipeline_archive") //reading from archived data
                val dfIntermediate = execSparkSql(spark, inputMap).repartition(10)
                println("second (insert) ")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting second (insert)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(2))
                inputMap.put(DFrepartitionArg, "2")
                inputMap.put(saveModeArg, "append")
                inputMap.put(tableNameArg, "tempDb.pipeline_temp_1")
                insertDeltaToHive(dfIntermediate, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed second (insert)\n")
                fileOutputStream.close
                println("third (archival)")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Starting third (archival)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(3))
                snapShotHive(dfIntermediate, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed third (archival)\n")
                fileOutputStream.close
              }
              case value if value == subJobNames(3) => {
                inputMap.put(sqlStringArg, s"select * from tempDb.temp_pipeline_archive")
                val dfIntermediate = execSparkSql(spark, inputMap).repartition(10)
                println("third (archival)")
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                println("third (archival)")
                fileOutputStream.writeBytes("Starting third (archival)\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(3))
                snapShotHive(dfIntermediate, spark, inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Completed third (archival)\n")
                fileOutputStream.close
              }
              case value if value == subJobNames(4) => {
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Sending Mail\n")
                fileOutputStream.close
                inputMap.put(currentJobNameArg, subJobNames(4))
                val failedJobRunID=dfStatus.select("job_run_id").collect.map(_(0).toString).toList(0)
                inputMap.put(runDateArg,runDate)
                inputMap.put(pwdArg,pwd)
                inputMap.put(jobRunIdArg,failedJobRunID)
                statusMail(spark,inputMap)
                fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                fileOutputStream.writeBytes("Status mail sent\n")
                fileOutputStream.close
              }
            }
          }
        }
      }
    }
  }
  catch {
    case e: Exception => {
      println(s"Error occurred while running job -> ${e.printStackTrace}")
      fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
      fileOutputStream.writeBytes("Error occurred while running job\n")
      fileOutputStream.close
    }
  }
  inputMap(currentJobNameArg) match
    {
    case value if value == subJobNames(4) => println("Failed while sending mail, Mail will be sent with previously used job id.")
    case _ => {
      fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
      fileOutputStream.writeBytes("Sending Mail\n")
      fileOutputStream.close
      inputMap.put(currentJobNameArg, subJobNames(4))
      inputMap.put(runDateArg,runDate)
      inputMap.put(pwdArg,pwd)
      inputMap.put(jobRunIdArg,jobRunID)
      statusMail(spark,inputMap)
      fileOutputStream = fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
      fileOutputStream.writeBytes("Status mail sent\n")
      fileOutputStream.close
    }
  }

}
}
