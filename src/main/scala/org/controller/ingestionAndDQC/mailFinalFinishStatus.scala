package org.controller.ingestionAndDQC

import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._

object mailFinalFinishStatus extends sparkOpener {
def main(args:Array[String]):Unit= {
  val spark = sparkSessionOpen()
  spark.sparkContext.setLogLevel("ERROR")
  val inputMap = collection.mutable.Map[String, String]()
  for (arg <- args)
    inputMap.put(arg.split("=", 2)(0), arg.split("=", 2)(1))
  val processingDate = inputMap("processingDate")
  val pwd = inputMap("pwd")
  val jobRunDate = dateFormat.format(new java.util.Date)
  val jobRunId = jobRunIDDateFormat.format(new java.util.Date)
  val logFile=s"${mailFinalFinishStatusLogPath}${jobRunId}.log"
  var writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
  writerObject.writeBytes(s"Starting  ${jobNameGoldRunStatusMail} \n")
  writerObject.writeBytes(s"Entering entry in  ${auditTable} for start status. \n")
  writerObject.close
  inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameGoldRunStatusMail}',current_timestamp(),'${statusStarted}')") // job run date is supposed too be processing date, because of testing , it's given separately
  execSparkSql(spark, inputMap)
  writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
  writerObject.writeBytes(s"Entered entry in  ${auditTable} for start status. \n")
  writerObject.writeBytes(s"Reading last run status of ${jobNameSilverToGold} from ${auditTable}. \n")
  writerObject.close
  inputMap.put(sqlStringArg, s"select * from ${auditTable} where job_run_date='${jobRunDate}' and job_sub_name='${jobNameSilverToGold}' order by job_status_entry_time desc limit 1") //'${timeStampDateFormat.format(new Date)}'
  val finalJobRanDF = execSparkSql(spark, inputMap)
  writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
  writerObject.writeBytes(s"Read last run status of ${jobNameSilverToGold} from ${auditTable}. \n ${finalJobRanDF.count match {case value if value == 0.asInstanceOf[Long] => "Silver to gold load job hasn't been run yet" case _ => s"Silver to gold load run job has previously ran with jobID ${finalJobRanDF.select("job_run_id").collect.map(_(0).toString).toList(0)} \n"}}")
  writerObject.close
  val goldLoadStatus = finalJobRanDF.count match {
    case value if value == 0.asInstanceOf[Long] => "NOT YET RUN"
    case _ => finalJobRanDF.select("job_status").collect.map(_ (0).toString).toList(0)
  }
  writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
  writerObject.writeBytes(s"Last ran job status is ${goldLoadStatus}. \n")
  writerObject.writeBytes(s"Building mail for the same.\n")
  writerObject.close
  inputMap.put(fromMailIdArg, "driftking9696@outlook.in")
  inputMap.put(toMailIdsArg, "driftking9696@outlook.in,driftking9696@gmail.com")
  inputMap.put(fromMailIdPwdArg, pwd)
  try {
    goldLoadStatus match {
      case value if (value == statusFinished || value == statusSuccess) => {
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
        writerObject.writeBytes(s"Building Success mail. \n")
        writerObject.close
        inputMap.put(mailSubjectArg, s"Silver to Gold load Successful for ${processingDate}")
        inputMap.put(mailBodyArg, s"Silver to Gold load Successful for ${processingDate}. No further action required.")
      }
      case value if value == statusFailure => {
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
        writerObject.writeBytes(s"Building Failure mail . \n")
        writerObject.close
        inputMap.put(mailSubjectArg, s"Silver to Gold load Failed for  ${processingDate}.")
        inputMap.put(mailBodyArg, s"Silver to Gold load Failed for  ${processingDate}. Look at the attached log for more info on reason for failure.")
        inputMap.put(hdfsDomain, hdfsDomainLocal)
        inputMap.put(mailAttachmentPathArg, s"${silverToGoldLogPath}${finalJobRanDF.select("job_run_id").collect.map(_ (0).toString).toList(0)}.log")
        inputMap.put(mailAttachmentFileNameArg, s"${finalJobRanDF.select("job_run_id").collect.map(_ (0).toString).toList(0)}.log")
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
        writerObject.writeBytes(s"Log for failed job will be attached in the mail. Log file's original path is ${inputMap(mailAttachmentPathArg)} \n")
        writerObject.close
      }
      case value if value == statusStarted => {
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
        writerObject.writeBytes(s"Previously ran job got stuck. Building mail. \n")
        writerObject.close
        inputMap.put(mailSubjectArg, s"Silver to Gold load stuck for ${processingDate}")
        inputMap.put(mailBodyArg, s"Silver to Gold load stuck for ${processingDate}. Please kill existing job and re run load again.")
      }
      case _ => {
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
        writerObject.writeBytes(s"No job has been run previously for today. Building mail. \n")
        writerObject.close
        inputMap.put(mailSubjectArg, s"Silver to Gold load not yet run for ${processingDate}")
        inputMap.put(mailBodyArg, s"Silver to Gold load not yet run for ${processingDate}. Please run the job to push data to downstream.")
      }
    }

  goldLoadStatus match {
    case value if value == statusFailure => mainSenderWithAttachment(inputMap); println("Mail will be sent with attachment")
    case _ => mainSender(inputMap); println("Mail will be sent without attachment")
  }
    inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameGoldRunStatusMail}',current_timestamp(),'${statusSuccess}')") // job run date is supposed too be processing date, because of testing , it's given separately
    execSparkSql(spark, inputMap)
  }
  catch
    {
      case e:Exception => {
        inputMap.put(sqlStringArg, s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameGoldRunStatusMail}',current_timestamp(),'${statusFailure}')") // job run date is supposed too be processing date, because of testing , it's given separately
        execSparkSql(spark, inputMap)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFile)
        writerObject.writeBytes(s"Error has occurred while running job. \n")
        writerObject.writeBytes(s"Stack trace: ${e.printStackTrace}. \n")
        writerObject.close
      }
    }
  }
}
