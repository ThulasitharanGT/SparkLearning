package org.controller.ingestionAndDQC

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.util.Date

import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._


object mailBronzeVsSilverStats extends sparkOpener {
  def main(args:Array[String]):Unit={
    val spark=sparkSessionOpen()
    val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
    val pwd=inputMap("pwd")
    val processingDate=inputMap("processingDate")
    val jobRunId=jobRunIDDateFormat.format(new Date)
    val jobRunDate=dateFormat.format(new Date)
    val logFilePath=s"${statsMailJobLogPath}${jobRunId}.log"
    var writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Starting ${jobNameBronzeVsSilverStatsStatusMail} for ${jobRunDate}\n")
    writerObject.writeBytes(s"Entering entry in audit table for ${jobNameBronzeVsSilverStatsStatusMail} for ${jobRunDate}\n")
    writerObject.close
    inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameBronzeVsSilverStatsStatusMail}',current_timestamp(),'${statusStarted}')")//'${timeStampDateFormat.format(new Date)}'
    execSparkSql(spark,inputMap)
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Entry entered in audit table for ${jobNameBronzeVsSilverStatsStatusMail} for ${jobRunDate}\n")
    writerObject.close
    inputMap.put(basePathArg,s"${bronzeVsSilverStatsBasePath}")
    inputMap.put(pathOption,s"${bronzeVsSilverStatsBasePath}job_run_date=${processingDate}")
    inputMap.put(fileFormatArg,parquetFileFormatArg)
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Reading stats of last run Job\n")
    writerObject.close
    val currentDayStatsDF=readDF(spark,inputMap).withColumn("rankCol",dense_rank.over(Window.partitionBy("job_run_id").orderBy(desc("job_run_id")))).filter("rankCol=1").drop("rankCol")
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Read stats of last run Job with JobID ${currentDayStatsDF.select("job_run_id").distinct.collect.toList(0)}\n")
    writerObject.close
    var comparisonStatus=""
    currentDayStatsDF.filter("difference_prev_day_bronze_vs_curr_day_bronze !=0").count match
    {
      case value if value == 0.asInstanceOf[Long] => {comparisonStatus = statusSuccess ;inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameBronzeVsSilverStatsStatusMail}',current_timestamp(),'${statusFinished} - Success')")/*'${timeStampDateFormat.format(new Date)}'*/;execSparkSql(spark,inputMap)}
      case value if value != 0.asInstanceOf[Long] => {comparisonStatus = statusFailure ;inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameBronzeVsSilverStatsStatusMail}',current_timestamp(),'${statusFinished} - Failure')")/*'${timeStampDateFormat.format(new Date)}'*/;execSparkSql(spark,inputMap)}
    }
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Comparison stats ${comparisonStatus} \n${comparisonStatus match {case value if value == statusSuccess => "Success mail will be sent" case value if value == statusFailure => "Failure mail with attachment will be sent" }} \n")
    writerObject.close
    var messageBody=""
    comparisonStatus match
    {
      case value if value == statusSuccess =>
      {
        messageBody=s"Job ${jobName} - ${jobRunId} stats check ${statusSuccess} for ${jobRunDate}\n No further action is required"
        inputMap.put(mailSubjectArg,s"Job ${jobName} - ${jobRunId} ${statusSuccess} for ${jobRunDate}")
        inputMap.put(mailBodyArg,messageBody)
        inputMap.put(fromMailIdArg,s"driftking9696@outlook.in")
        inputMap.put(toMailIdsArg,s"driftking9696@outlook.in,driftking9696@gmail.com")
        inputMap.put(fromMailIdPwdArg,pwd)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Job ${jobName} stats check ${statusSuccess} for ${jobRunDate}\nNo further action is required\nSending mail\n")
        writerObject.close
        mainSender(inputMap)
        println("withoutAttachment")
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Mail has been sent")
        writerObject.close
      }
      case value if value == statusFailure =>
      {
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Job ${projectConstants.jobName} stats check ${statusFailure} for ${jobRunDate}\n")
        writerObject.close
        val nonMatchingRecords=currentDayStatsDF.filter("difference_prev_day_bronze_vs_curr_day_bronze !=0").select("job_run_id","job_name","job_sub_name","data_date","previous_day_Count_bronze","job_Run_day_Count_bronze","difference_prev_day_bronze_vs_curr_day_bronze","job_run_date").collect
        val jobRunId=nonMatchingRecords(0)(0)
        val jobName=nonMatchingRecords(0)(1)
        val jobSubName=nonMatchingRecords(0)(2)
        messageBody=s"Job ${jobSubName} in ${jobName} failed for ${jobRunDate} \n<h1>Dates and count information are as follows : </h1>"
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Dates and count information are as follows : \n")
        writerObject.close
        for(nonMatchingRecord <- nonMatchingRecords)
          messageBody=messageBody+s"\n<h3>Data date = ${nonMatchingRecord(3)} </h3> \n Bronze count when checked previous day = ${nonMatchingRecord(4)} \n Bronze count today = ${nonMatchingRecord(5)}\n Extra records found = ${nonMatchingRecord(6)}"
        inputMap.put(mailSubjectArg,s"Job ${jobSubName} - ${jobRunId} in ${jobName} failed for ${jobRunDate}")
        inputMap.put(mailBodyArg,messageBody)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"${messageBody.replaceAll("<h3>","").replaceAll("</h3>","")}\n")
        writerObject.close
        import sys.process._
        inputMap.put(fileFormatArg,csvFileFormatArg)
        inputMap.put(delimiterOption,delimiterOr)
        inputMap.put(headerOption,stringTrue)
        inputMap.put(pathOption,failureStatsTempFilePath)
        inputMap.put(saveModeArg,overWriteMode)
        inputMap.put(DFrepartitionArg,"2")
        inputMap.put(coalesceArg,"1")
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Writing stats file in ${failureStatsTempFilePath} path to attach it in mail\n")
        writerObject.close
        writeDF(spark,inputMap,currentDayStatsDF)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Wrote stats file in ${failureStatsTempFilePath} path to attach it in mail\n")
        writerObject.close
       // val filesInPath=s"hdfs dfs -ls -t ${bronzeVsSilverStatsBasePath}/job_run_date=${processingDate}"!!  //-t is not working in local hadoop, do a workaround by writing stats DF as CSV and Attaching that in mail
        inputMap.put(sysCommandArg,s"hdfs dfs -rm ${failureStatsTempFilePath}_SUCCESS")
        val pickIndex= sysCommandExecuter(inputMap) match { case true => 1 case false => 2}
        inputMap.put(sysCommandArg,s"hdfs dfs -ls ${failureStatsTempFilePath}")
        val filesInPath=sysCommandExecuterWithOutput(inputMap)
        val filePath=filesInPath.split("\n")(pickIndex)
        val filePathFinal= filePath.contains("hdfs://") match {case true => filePath.substring(filePath.indexOf("/")-5,filePath.size) case false => filePath.substring(filePath.indexOf("/"),filePath.size)}
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"File in this path ${filePathFinal} will be attached as log_${jobRunId}.csv in mail\n")
        writerObject.close
        inputMap.put(hdfsDomain,"hdfs://localhost:8020/")
        inputMap.put(mailAttachmentPathArg,s"${filePathFinal}")
        inputMap.put(mailAttachmentFileNameArg,s"log_${jobRunId}.csv")
        inputMap.put(fromMailIdArg,s"driftking9696@outlook.in")
        inputMap.put(toMailIdsArg,s"driftking9696@outlook.in,driftking9696@gmail.com")
        inputMap.put(fromMailIdPwdArg,pwd)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Job ${jobName} stats check ${statusFailure} for ${jobRunDate}\nSending mail\n")
        writerObject.close
        mainSenderWithAttachment(inputMap)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Mail sent. Check mail for stats file\n")
        writerObject.close
        println("withAttachment")
      }
      case _ => println("Mail wont be sent. \nInvalid selection.")
    }
    inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameBronzeVsSilverStatsStatusMail}',current_timestamp(),'${statusFinished}')")/*'${timeStampDateFormat.format(new Date)}'*/
    execSparkSql(spark,inputMap)
  }
}
