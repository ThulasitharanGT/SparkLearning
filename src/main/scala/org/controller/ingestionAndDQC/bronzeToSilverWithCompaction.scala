package org.controller.ingestionAndDQC

import java.util.{Calendar,Date}
import org.apache.hadoop.fs.{FileSystem, Path}

import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._

// second level of bronze, Just compaction is done here, silver phase 2 is under planning
object bronzeToSilverWithCompaction extends sparkOpener{
  def main(args:Array[String]):Unit ={
   val spark=sparkSessionOpen()
    spark.sparkContext.setLogLevel("ERROR")
   val inputMap=collection.mutable.Map[String,String]()
   for(arg <- args)
     inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
   val processingDate=inputMap("processingDate")            //"2020-08-03" // 1st data is processed on second
   val calenderInstance = Calendar.getInstance()
   calenderInstance.setTime(dateFormat.parse(processingDate))
   calenderInstance.add(Calendar.DAY_OF_MONTH,-1)
   val dataDate=dateFormat.format(calenderInstance.getTime)
   val jobRunId=jobRunIDDateFormat.format(new Date)
   val logFilePathForBronzeToSilver=s"${bronzeToSilverLogPath}${jobRunId}.log"
    var writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
    writerObject.writeBytes(s"Starting Job to copy data from bronze to silver for ${dataDate} on ${processingDate} \n")
    writerObject.writeBytes(s"Putting entry in Audit table for ${jobNameBatch} \n")
    writerObject.close
    inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${processingDate}') values(${jobRunId},'${jobName}','${jobNameBatch}',current_timestamp(),'${statusStarted}')")//'cast(${timeStampDateFormat.format(new Date)} as timestamp)'
    execSparkSql(spark,inputMap)
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
    writerObject.writeBytes("Audit table entry successful")
    writerObject.close
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    try {
      val dirSize = fs.getContentSummary(new Path(s"${bronzeBasePath}/processDate=${dataDate}")).getLength // in bytes
      val numOfFiles = Math.ceil(dirSize / (1024 * 1024)) match {
        case value if value != 0.asInstanceOf[Long] => value.toInt
        case _ => 1.toInt
      } //(to MB) it should not be more than 1 mb , so it can be nearer to 900 kb
      inputMap.put(fileFormatArg, deltaFileFormat)
      inputMap.put(basePathArg, bronzeBasePath)
      inputMap.put(pathOption, s"${bronzeBasePath}/processDate=${dataDate}") // reads previous day's data for input date
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
      writerObject.writeBytes(s"Reading bronze data for ${dataDate} from ${bronzeBasePath}/processDate=${dataDate}")
      writerObject.close
      val sourceFromBronzeForCurrentDateDF = readDF(spark, inputMap).repartition(10)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
      writerObject.writeBytes(s"Read bronze data for ${dataDate} from ${bronzeBasePath}/processDate=${dataDate}")
      writerObject.close
      inputMap.put(basePathArg, bronzeBasePath)
      inputMap.put(pathOption, s"${silverBasePath}")
      inputMap.put(coalesceArg, s"${numOfFiles}")
      inputMap.put(partitionByFlag, stringTrue)
      inputMap.put(partitionByColumns, "processDate")
      inputMap.put(saveModeArg, appendMode)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
      writerObject.writeBytes(s"Saving bronze data for ${dataDate} from ${bronzeBasePath}/processDate=${dataDate} to silver for ${dataDate} from ${silverBasePath}/processDate=${dataDate}")
      writerObject.close
      // saving the files with compaction
      writeDF(spark, inputMap, sourceFromBronzeForCurrentDateDF)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
      writerObject.writeBytes(s"Save for silver data for ${dataDate} into ${silverBasePath}/processDate=${dataDate} complete")
      writerObject.close
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
      writerObject.writeBytes(s"Putting completed entry into audit table for  ${jobNameBatch}")
      writerObject.close
      inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${processingDate}') values(${jobRunId},'${jobName}','${jobNameBatch}',current_timestamp(),'${statusFinished}')")
      execSparkSql(spark, inputMap)
      writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
      writerObject.writeBytes(s"Entry into audit table for  ${jobNameBatch} successful")
      writerObject.close
    }
        catch
        {
          case e:Exception => {
            println(e.printStackTrace)
            inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${processingDate}') values(${jobRunId},'${jobName}','${jobNameBatch}',current_timestamp(),'${statusFailure}')")
            execSparkSql(spark,inputMap)
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePathForBronzeToSilver)
            writerObject.writeBytes(s"Job failed  - ${jobNameBatch} \nStack trace : \n${e.printStackTrace}")
            writerObject.writeBytes(s"Updated failure status in audit table\n")
            writerObject.close
          }
        }
  }
}
