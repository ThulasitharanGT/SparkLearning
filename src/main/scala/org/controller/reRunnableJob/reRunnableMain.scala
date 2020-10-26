package reRunnableJob

import org.util.SparkOpener
import java.text.SimpleDateFormat
import java.util.Date
import  org.controller.reRunnableJob.constants._
import  org.controller.reRunnableJob.jobFuctions._
import  org.controller.reRunnableJob.readWriteUtil._

object reRunnableMain extends SparkOpener{
def main(args:Array[String]):Unit=
  {
    val spark=SparkSessionLoc("")
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val dateFormatterJobId = new SimpleDateFormat("yyyyMMddhhmmss")
    val inputMap=collection.mutable.Map[String,String]()
    val date = new Date()
    val runDate=dateFormatter.format(date)
    val jobRunID=dateFormatterJobId.format(date)
    //val runDate="2020-05-15"
    /*val redshiftUrl = ""//jdbc url
    val redshiftUser = "" //jdbc user name
    val redshiftPwd = "" // jdbc password
    val redshiftDriver = "com.amazon.redshift.jdbc42.Driver"*/
    val mainJobName="temp_load"
    val logPathArg="logPath"
    val hdfsDomain="hdfs.domain"
    val subJobNames=Seq("RedshiftRead","temp_archival","HiveInsert","HiveSnapshotArchival")
    val logPath=s"hdfs:///user/raptor/hadoop/logs/${dateFormatterJobId.format(date)}.log"
    val hdfsCurrentDomain="hdfs://localhost:8020/"
    inputMap.put(logPathArg,logPath)
    inputMap.put(hdfsDomain,hdfsCurrentDomain)
    inputMap.put(mainJobNameArg,mainJobName)
    inputMap.put(runDateArg,runDate)
    inputMap.put(jobRunIdArg,jobRunID)
    inputMap.put(sqlStringArg,s"select * from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}')")
    val dfCount=execSparkSql(spark,inputMap).count
    println(dfCount)
    try{
      dfCount match
      {
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
          var fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Starting one (read) from redshift\n")
          fileOutputStream.close
          // val redshiftDF=loadFromRedshift(spark,inputMap)
          val redshiftDF=loadFromHttp(spark,inputMap)
          println("1.5 Archival ")
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Completed one (read) from redshift\n")
          fileOutputStream.close
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Starting 1.5 Archival")
          fileOutputStream.close
          inputMap.put(currentJobNameArg, subJobNames(1))
          archivalForRedshift(redshiftDF,spark,inputMap)
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Completed 1.5 Archival\n")
          fileOutputStream.close
          println("second (insert) ")
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Starting second (insert)\n")
          fileOutputStream.close
          inputMap.put(currentJobNameArg, subJobNames(2))
          inputMap.put(DFrepartitionArg, "2")
          inputMap.put(saveModeArg, "append")
          inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
          insertDeltaToHive(redshiftDF,spark,inputMap)
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Completed second (insert)\n")
          fileOutputStream.close
          println("third (archival)")
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Starting third (archival)\n")
          fileOutputStream.close
          inputMap.put(currentJobNameArg, subJobNames(3))
          snapShotHive(redshiftDF,spark,inputMap)
          fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Completed third (archival)\n")
          fileOutputStream.close
        }
        case value if value > 0.asInstanceOf[Long] => {
          inputMap.put(sqlStringArg, s"select * from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}') order by job_updation_date desc limit 1")
          val dfStatus=execSparkSql(spark,inputMap)
          dfStatus.select("sub_job_name").filter(s"job_status ='${failedString}'").count match
          {
            case value if value == 0.asInstanceOf[Long] =>
            {
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
              var fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Starting one (read) from redshift\n")
              fileOutputStream.close
              // val redshiftDF=loadFromRedshift(spark,inputMap)
              val redshiftDF=loadFromHttp(spark,inputMap)
              println("1.5 Archival ")
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Completed one (read) from redshift\n")
              fileOutputStream.close
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Starting 1.5 Archival")
              fileOutputStream.close
              inputMap.put(currentJobNameArg, subJobNames(1))
              archivalForRedshift(redshiftDF,spark,inputMap)
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Completed 1.5 Archival\n")
              fileOutputStream.close
              println("second (insert) ")
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Starting second (insert)\n")
              fileOutputStream.close
              inputMap.put(currentJobNameArg, subJobNames(2))
              inputMap.put(DFrepartitionArg, "2")
              inputMap.put(saveModeArg, "append")
              inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
              insertDeltaToHive(redshiftDF,spark,inputMap)
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Completed second (insert)\n")
              fileOutputStream.close
              println("third (archival)")
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Starting third (archival)\n")
              fileOutputStream.close
              inputMap.put(currentJobNameArg, subJobNames(3))
              snapShotHive(redshiftDF,spark,inputMap)
              fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
              fileOutputStream.writeBytes("Completed third (archival)\n")
              fileOutputStream.close
            }
            case _ => {
              val failedJobName=dfStatus.select("sub_job_name").filter(s"job_status ='${failedString}'").collect.map(_(0).asInstanceOf[String]).toList(0)
              failedJobName match
              {
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
                  var fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting one (read) from redshift\n")
                  fileOutputStream.close
                  // val redshiftDF=loadFromRedshift(spark,inputMap)
                  val redshiftDF=loadFromHttp(spark,inputMap)  // we don't have redshift so we wget and run the data
                  println("1.5 Archival ")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed one (read) from redshift\n")
                  fileOutputStream.close
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting 1.5 Archival")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(1))
                  archivalForRedshift(redshiftDF,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed 1.5 Archival\n")
                  fileOutputStream.close
                  println("second (insert) ")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting second (insert)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(2))
                  inputMap.put(DFrepartitionArg, "2")
                  inputMap.put(saveModeArg, "append")
                  inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
                  insertDeltaToHive(redshiftDF,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed second (insert)\n")
                  fileOutputStream.close
                  println("third (archival)")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting third (archival)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(3))
                  snapShotHive(redshiftDF,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
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
                  var fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting one (read) from redshift\n")
                  fileOutputStream.close
                  // val redshiftDF=loadFromRedshift(spark,inputMap)
                  val redshiftDF=loadFromHttp(spark,inputMap)
                  println("1.5 Archival ")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed one (read) from redshift\n")
                  fileOutputStream.close
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting 1.5 Archival")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(1))
                  archivalForRedshift(redshiftDF,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed 1.5 Archival\n")
                  fileOutputStream.close
                  println("second (insert) ")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting second (insert)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(2))
                  inputMap.put(DFrepartitionArg, "2")
                  inputMap.put(saveModeArg, "append")
                  inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
                  insertDeltaToHive(redshiftDF,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed second (insert)\n")
                  fileOutputStream.close
                  println("third (archival)")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting third (archival)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(3))
                  snapShotHive(redshiftDF,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed third (archival)\n")
                  fileOutputStream.close
                }
                case value if value == subJobNames(2) => {
                  inputMap.put(sqlStringArg, s"select * from tempDb.temp_pipeline_archive") //reading from archived data
                  val dfIntermediate=execSparkSql(spark,inputMap).repartition(10)
                  println("second (insert) ")
                  var fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting second (insert)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(2))
                  inputMap.put(DFrepartitionArg, "2")
                  inputMap.put(saveModeArg, "append")
                  inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
                  insertDeltaToHive(dfIntermediate,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed second (insert)\n")
                  fileOutputStream.close
                  println("third (archival)")
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Starting third (archival)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(3))
                  snapShotHive(dfIntermediate,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed third (archival)\n")
                  fileOutputStream.close
                }
                case value if value == subJobNames(3) => {
                  inputMap.put(sqlStringArg, s"select * from tempDb.temp_pipeline_archive")
                  val dfIntermediate=execSparkSql(spark,inputMap).repartition(10)
                  println("third (archival)")
                  var fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  println("third (archival)")
                  fileOutputStream.writeBytes("Starting third (archival)\n")
                  fileOutputStream.close
                  inputMap.put(currentJobNameArg, subJobNames(3))
                  snapShotHive(dfIntermediate,spark,inputMap)
                  fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
                  fileOutputStream.writeBytes("Completed third (archival)\n")
                  fileOutputStream.close
                }
              }
            }
          }
        }
      }
    }
    catch
      {
        case e:Exception=>{
          println(s"Error occurred while running job -> ${e.printStackTrace}")
          val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
          fileOutputStream.writeBytes("Error occured while running job\n")
          fileOutputStream.close
        }
      }
    // put it in a separate object
    inputMap.put(sqlStringArg,s"select sub_job_name,job_status from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}') order by job_updation_date desc limit 1")
    val statusCheckDf=execSparkSql(spark,inputMap)
    val lastJobName=statusCheckDf.select("sub_job_name").collect.map(_(0).asInstanceOf[String]).toList(0)
    val lastJobStatus=statusCheckDf.select("job_status").collect.map(_(0).asInstanceOf[String]).toList(0)
    lastJobStatus match
    {
      case  value if value ==completedString => lastJobName match
      {
        case value if value == subJobNames(3) => inputMap.put(messageSubjectArg,s"Job completed till ${value} successfully for ${runDate}")
          inputMap.put(messageContentArg,s"Job completed till ${value} successfully for ${runDate} \n No Failures found")
        case _ => inputMap.put(messageSubjectArg,s"Job completed till ${value} successfully ")
          inputMap.put(messageContentArg,s"Job completed till ${value} successfully for ${runDate} \n Re run again to resume the job from failed point")
      }
      case  value if value ==failedString => inputMap.put(messageSubjectArg,s"Job failed in ${lastJobName} for ${runDate}")
        inputMap.put(messageContentArg,s"Job failed in ${lastJobName} for ${runDate} \n Re run again to resume the job from failed point")
      case  value if value ==runningString => inputMap.put(messageSubjectArg,s"Job stuck in ${lastJobName}  for ${runDate}")
        inputMap.put(messageContentArg,s"Job stuck in ${lastJobName} for ${runDate} \n Update record to failed manually or insert a new entry with status as failed for this job and re run again to resume the job from failed point")
    }
    inputMap.put(fromEmailProperty,"driftking9696@outlook.in")
    inputMap.put(toEmailProperty,"driftking9696@outlook.in,driftking9696@gmail.com")
    inputMap.put(fromPasswordProperty,"")// pwd
    inputMap.put(filePathArg,inputMap(logPathArg))
    inputMap.put(attachmentNameArg,s"log_${jobRunID}.txt")
    mainSenderWithAttachment(inputMap)
  }
}
