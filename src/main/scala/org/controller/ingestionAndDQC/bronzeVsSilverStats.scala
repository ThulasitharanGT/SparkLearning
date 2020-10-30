package org.controller.ingestionAndDQC

import java.util.{Calendar, Date}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{count, lit}
import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._

object bronzeVsSilverStats extends sparkOpener {
def main(args:Array[String]):Unit = {
  val spark=sparkSessionOpen()
  spark.sparkContext.setLogLevel("ERROR")
  val inputMap=collection.mutable.Map[String,String]()
  for(arg <- args)
    inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
  val calenderInstance = Calendar.getInstance()
  val processingDate=inputMap("processingDate")
  calenderInstance.setTime(dateFormat.parse(processingDate))
  calenderInstance.add(Calendar.DAY_OF_MONTH,-1)
  val dataDate=dateFormat.format(calenderInstance.getTime)
  val jobRunId=jobRunIDDateFormat.format(new Date)
  val logFilePathForStatsBronzeVsSilver=s"${statsJobLogPath}${jobRunId}.log"
  var writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
  writerObject.writeBytes(s"starting  ${jobNameStatsBatch} \n")
  writerObject.close
  try
    {
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Putting entry into audit table for  ${jobNameStatsBatch} \n")
      writerObject.close
      inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${processingDate}') values(${jobRunId},'${jobName}','${jobNameStatsBatch}',current_timestamp(),'${statusStarted}')")
      execSparkSql(spark, inputMap)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Entered entry into audit table for  ${jobNameStatsBatch} \n")
      writerObject.close
      inputMap.remove(basePathArg) // removing basePathArg, to read entire table
      inputMap.put(pathOption,s"${bronzeBasePath}")
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Taking count stats for bronze table in path ${bronzeBasePath}\n")
      writerObject.close
      inputMap.put(fileFormatArg, deltaFileFormat)
      inputMap.put(pathOption, s"${bronzeBasePath}")
      val bronzeFullDF=readDF(spark,inputMap).repartition(100).filter(s"processDate <=to_date('${dataDate}')").groupBy("processDate").agg(count("*").as("rowCount")).repartition(100)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Count stats for bronze table in path ${bronzeBasePath} is completed\n")
      writerObject.close
      inputMap.put(pathOption,s"${silverBasePath}")
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Taking count stats for silver table in path ${silverBasePath}\n")
      writerObject.close
      val silverFullDF=readDF(spark,inputMap).repartition(100).groupBy("processDate").agg(count("*").as("rowCount")).repartition(100)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Count stats for bronze table in path ${silverBasePath} is completed\n")
      writerObject.writeBytes("calculating Stats bronze vs silver\\n")
      writerObject.close
      val resultDF=bronzeFullDF.as("bronze").join(silverFullDF.as("silver"),Seq("processDate"),"full_outer").selectExpr("processDate","bronze.rowCount as src_rowcount","silver.rowCount as dest_rowcount","bronze.rowCount - silver.rowCount as difference").repartition(10)
      inputMap.put(sqlStringArg,s"select data_date,job_Run_day_Count_bronze from ${statsTable} where to_date(job_run_date)=to_date('${dataDate}')  order by data_date ")
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Reading previous day stats from  ${statsTable} for date ${dataDate}\n")
      writerObject.close
      val statsForPreviousDayDF=execSparkSql(spark,inputMap)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Read previous day stats from  ${statsTable} for date ${dataDate}\n")
      writerObject.close
      val finalResult_1=resultDF.filter(s"to_date(processDate) != to_date('${dataDate}')").as("result").join(statsForPreviousDayDF.as("prev_stats"),resultDF("processDate") === statsForPreviousDayDF("data_date")).selectExpr("result.processDate as data_date","prev_stats.job_Run_day_Count_bronze","result.src_rowcount","result.dest_rowcount","result.difference as difference_job_Run_day_bronze_vs_silver","case when prev_stats.job_Run_day_Count_bronze =0 then 0 when prev_stats.job_Run_day_Count_bronze-result.src_rowcount <=0 then prev_stats.job_Run_day_Count_bronze-result.src_rowcount else 0 end difference_prev_day_bronze_vs_curr_day_bronze")
      val finalResult_2=resultDF.filter(s"to_date(processDate) = to_date('${dataDate}')").as("result").selectExpr("result.processDate as data_date","cast(0 as bigint) as job_Run_day_Count_bronze","result.src_rowcount","result.dest_rowcount","result.difference as difference_job_Run_day_bronze_vs_silver").selectExpr("*","case when job_Run_day_Count_bronze =0 then 0 when job_Run_day_Count_bronze-result.src_rowcount <=0 then job_Run_day_Count_bronze-result.src_rowcount else 0 end difference_prev_day_bronze_vs_curr_day_bronze")
      val finalResult=finalResult_1.union(finalResult_2)
      val statsTableColumnSeq=Seq("job_run_id","job_name","job_sub_name","current_timestamp() as job_status_entry_time","job_status","cast(finalResult.data_date as date) data_date","finalResult.job_Run_day_Count_bronze as previous_day_Count_bronze","finalResult.src_rowcount as job_Run_day_Count_bronze","finalResult.dest_rowcount as job_Run_day_Count_silver","finalResult.difference_job_Run_day_bronze_vs_silver ","finalResult.difference_prev_day_bronze_vs_curr_day_bronze ","job_run_date")
      import org.apache.spark.sql.types._
      inputMap.put(pathOption,s"${bronzeVsSilverStatsBasePath}")
      inputMap.put(fileFormatArg,parquetFileFormatArg)
      inputMap.put(DFrepartitionArg,"2")
      inputMap.put(coalesceArg,"1")
      inputMap.put(partitionByFlag,stringTrue)
      inputMap.put(saveModeArg,appendMode)
      inputMap.put(partitionByColumns,"job_run_date") // comma separated columns, It'll be split inside the function
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Stats computed, preparing to write to final table ${statsTable} \n")
      writerObject.close
      val statsForTodayDF=finalResult.as("finalResult").withColumn("job_run_id",lit(jobRunId).cast(LongType)).withColumn("job_name",lit(jobName)).withColumn("job_sub_name",lit(jobNameStatsBatch))/*.withColumn("job_status_entry_time",lit(timeStampDateFormat.format(new Date)))*/.withColumn("job_status",lit(statusSuccess)).withColumn("job_run_date",lit(processingDate).cast(DateType)).selectExpr(statsTableColumnSeq:_*)
      writeDF(spark,inputMap,statsForTodayDF)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Stats written to ${statsTable} \n")
      writerObject.writeBytes(s"Inserting completed status for ${jobNameStatsBatch} in audit table \n")
      writerObject.close
      //.repartition(2).coalesce(1).write.mode("append").format("parquet").partitionBy("job_run_date").save(s"${bronzeVsSilverStatsBasePath}")
      inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${processingDate}') values(${jobRunId},'${jobName}','${jobNameStatsBatch}',current_timestamp(),'${statusFinished}')")
      execSparkSql(spark,inputMap)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
      writerObject.writeBytes(s"Completed status for ${jobNameStatsBatch} inserted in audit table \n")
      writerObject.close
    }
  catch
    {
      case e:Exception => {
        println(e.printStackTrace)
        inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${processingDate}') values(${jobRunId},'${jobName}','${jobNameStatsBatch}',current_timestamp(),'${statusFailure}')")
        execSparkSql(spark,inputMap)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
        writerObject.writeBytes(s"Job Failed for ${jobNameStatsBatch} \nStack Trace:\n ${e.printStackTrace}")
        writerObject.writeBytes(s"Updated failure status in audit table\n")
        writerObject.close
      }
    }
  writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
  writerObject.writeBytes(s"Refreshing partition metastore for ${statsTable}  by running alter table add partition command\n")
  writerObject.close
  inputMap.put(sqlStringArg,s"ALTER TABLE ${statsTable} ADD IF NOT EXISTS PARTITION (job_run_date='${processingDate}')")
  execSparkSql(spark,inputMap)
  writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForStatsBronzeVsSilver)
  writerObject.writeBytes(s"Alter table add partition command completed successfully\n")
  writerObject.close
}
}
