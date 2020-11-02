package  org.controller.reRunnableJob
import  org.controller.reRunnableJob.constants._
import  org.controller.reRunnableJob.readWriteUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import sys.process._

object jobFunctions {
  def loadFromRedshift(spark:SparkSession,inputMap:collection.mutable.Map[String,String]) ={
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    inputMap.put(dbTableOption, "(select * from /*red_shiftdb.redshift_table*/)t")
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running from load from redshift\n")
    var df:org.apache.spark.sql.DataFrame=null
    try{
      df=readDF(spark,inputMap)
    }
    catch
      {
        case e:Exception => println(e.printStackTrace)
          fileOutputStream.writeBytes("Error while running load from redshift\n")
          fileOutputStream.close
      }
    df match {
      case null => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Error while running load from redshift\n")
        fileOutputStream.close
        System.exit(9)
        //println("exit")
      }
      case _ => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Load from redshift successful\n")
        fileOutputStream.close
      }
    }
    df
  }
  def columnNameCorrection(df:org.apache.spark.sql.DataFrame)=
  {
    val currentColumns=df.columns.map(_.asInstanceOf[String])
    val correctedColumns=currentColumns/*map(x => x.substring(0,x.indexOf("(")))*/.map(_.trim).map(_.replace(" ","_"))
    df.toDF(correctedColumns:_*)
  }

  def loadFromHttp (spark:SparkSession,inputMap:collection.mutable.Map[String,String]) ={
    val colNames="Element,Number,Symbol,Weight,Boil,Melt,density_Vapour,fusion,diffusion_index".split(",").map(_.asInstanceOf[String])
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running from load from http\n")

    wgetRunner(inputMap) match// if the result is false exit
    {
      case false => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Error while downloading file from http\n")
        fileOutputStream.close
        System.exit(9)
        println("exit")
      }
      case true =>         fileOutputStream.writeBytes("Downloaded file successfully from http\n")
    }
    //overwriting configs for http here

    inputMap.put(pathOption,s"hdfs:///user/raptor/hadoop/data/data_${inputMap(jobRunIdArg)}.csv")
    s"hdfs dfs -put /home/raptor/temp/fileDownload/data_${inputMap(jobRunIdArg)}.csv hdfs:///user/raptor/hadoop/data/data_${inputMap(jobRunIdArg)}.csv"!
   // dbutils.fs.cp(inputMap(wgetFileNameArg),inputMap(filePathArg))
    var df:org.apache.spark.sql.DataFrame=null
    try{
      //df=columnNameCorrection(readDF(spark,inputMap))
      df=readDF(spark,inputMap).toDF(colNames:_*)
    }
    catch
      {
        case e:Exception => println(e.printStackTrace)
          fileOutputStream.writeBytes("Error while running load from http\n")
          fileOutputStream.close
      }
    df match {
      case null => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Error while running load from http\n")
        fileOutputStream.close
        System.exit(9)
        //println("exit")
      }
      case _ => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Load from http successful\n")
        fileOutputStream.close
      }
    }
    df
  }

  def archivalForRedshift(dfTemp:org.apache.spark.sql.DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String])={
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    inputMap.put(DFrepartitionArg, "2")
    inputMap.put(saveModeArg, "overwrite")
    inputMap.put(tableNameArg, "tempDb.temp_pipeline_archive")
    fileOutputStream.writeBytes("Running Archive redshift data in temp table\n ")
    try{
     //insertIntoHive(spark,inputMap,dfTemp.repartition(10))
      saveAsTableHive(spark,inputMap,dfTemp.repartition(10))
    }
    catch
      {
        case e:Exception =>
        {
          println(e.printStackTrace)
          inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          fileOutputStream.writeBytes("Archive redshift data in temp table failed\n")
          fileOutputStream.close
          System.exit(9)
          //println("exit")
        }
      }
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${successMessage}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    fileOutputStream.writeBytes("Archive redshift data in temp table Successful\n")
    fileOutputStream.close
  }

  def insertDeltaToHive(dfTemp:org.apache.spark.sql.DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String]) =
  {
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running insert into hive table \n")
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    inputMap.put(DFrepartitionArg, "2")
    inputMap.put(saveModeArg, "append")
    inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
    try{
      insertIntoHive(spark,inputMap ,dfTemp.withColumn("dataDate",lit(inputMap(runDateArg))).repartition(10))
    }
    catch
      {
        case e:Exception => {
          println("failed to write into HiveBase Table")
          inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          fileOutputStream.writeBytes("insert into hive table failed\n")
          fileOutputStream.close
          System.exit(9)
          //println("exit")
        }
      }
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    fileOutputStream.writeBytes("insert into hive table success\n")
    fileOutputStream.close
  }

  def snapShotHive(dfTemp:org.apache.spark.sql.DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String])=
  {
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running snapshot into hive table \n")
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    inputMap.put(DFrepartitionArg, "2")
    inputMap.put(saveModeArg, "append")
    inputMap.put(tableNameArg,"tempDb.pipeline_temp_2") // if you need dynamic table , name read it from a file and pass it dynamically from main funvtion like you do in count or data scheck
    try{
      insertIntoHive(spark,inputMap ,dfTemp.withColumn("dataDate",lit(inputMap(runDateArg))).withColumn("jobRunID",lit(inputMap(jobRunIdArg))).repartition(10))
    }
    catch
      {
        case e:Exception => {
          println("failed to write into hive archive Table")
          inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          fileOutputStream.writeBytes("snapshot into hive table failed\n")
          fileOutputStream.close
          System.exit(9)
          //println("exit")
        }
      }
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    fileOutputStream.writeBytes("snapshot into hive table success\n")
    fileOutputStream.close
  }
  def statusMail(spark:org.apache.spark.sql.SparkSession,inputMap:collection.mutable.Map[String,String])={
    val runDate=inputMap(runDateArg)
    val pwd=inputMap(pwdArg)
    val jobRunID=inputMap(jobRunIdArg)
    inputMap.put(sqlStringArg,s"select sub_job_name,job_status from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}') order by job_updation_date desc limit 1")
    val statusCheckDf=execSparkSql(spark,inputMap)
    val lastJobName=statusCheckDf.select("sub_job_name").collect.map(_(0).asInstanceOf[String]).toList(0)
    val lastJobStatus=statusCheckDf.select("job_status").collect.map(_(0).asInstanceOf[String]).toList(0)
    lastJobStatus match
    {
      case  value if value ==completedString => lastJobName match
      {
        case value if value == subJobNames(3) => inputMap.put(messageSubjectArg,s"Job completed till ${value} successfully for ${runDate}") // mail just sends status, if mail failed no issues. If hive snapshot fails then issue
          inputMap.put(messageContentArg,s"Job completed till ${value} successfully for ${runDate} \n No Failures found")
        case _ => inputMap.put(messageSubjectArg,s"Job completed till ${value} successfully ")
          inputMap.put(messageContentArg,s"Job completed till ${value} successfully for ${runDate} \n Re run again to resume the job from failed point")
      }
      case  value if value ==failedString => lastJobName match {
        case value if value == subJobNames(4) => {
          inputMap.put(sqlStringArg,s"select distinct sub_job_name,job_status,job_run_id,dense_rank() over(order by job_updation_date desc) as rankCol from (select distinct sub_job_name,job_status,job_run_id,job_updation_date from tempDb.temp_audit_table where to_date(run_date)=to_date('${runDate}') and sub_job_name='${subJobNames(3)}' order by job_updation_date  desc)a limit 1")
          val previousJobsDataDF=execSparkSql(spark,inputMap)
          previousJobsDataDF.show(false)
          val lastRunJobName=previousJobsDataDF.select("sub_job_name").collect.map(_(0).asInstanceOf[String]).toList(0)
          val lastRunJobID=previousJobsDataDF.select("job_run_id").collect.map(_(0).toString).toList(0)
          val lastRunJobStatus=previousJobsDataDF.select("job_status").collect.map(_(0).asInstanceOf[String]).toList(0)
          lastRunJobStatus match {
           case value if value.equals(completedString)=> {
           inputMap.put(messageSubjectArg,s"Job completed till ${value} successfully for ${lastRunJobID}")
          inputMap.put(messageContentArg, s"Job completed till ${lastRunJobName} for ${runDate} \n No further action is required")
          }
           case value =>{
             inputMap.put(messageSubjectArg,s"Job ${value} for ${lastRunJobID} in ${lastRunJobName}")
             inputMap.put(messageContentArg, s"Job ${value} for ${lastRunJobID} in ${lastRunJobName} \n Fishy scenario , please contact dev team to check in backend.")
           }
          }
        }
        case _ => {
          inputMap.put(messageSubjectArg, s"Job failed in ${lastJobName} for ${runDate}")
          inputMap.put(messageContentArg, s"Job failed in ${lastJobName} for ${runDate} \n Re run again to resume the job from failed point")
        }
      }
      case  value if value ==runningString => inputMap.put(messageSubjectArg,s"Job stuck in ${lastJobName}  for ${runDate}")
        inputMap.put(messageContentArg,s"Job stuck in ${lastJobName} for ${runDate} \n Update record to failed manually or insert a new entry with status as failed for this job and re run again to resume the job from failed point")
    }
    inputMap.put(fromEmailProperty,"driftking9696@outlook.in")
    inputMap.put(toEmailProperty,"driftking9696@outlook.in,driftking9696@gmail.com")
    inputMap.put(fromPasswordProperty,pwd)// pwd
    inputMap.put(filePathArg,inputMap(logPathArg))
    inputMap.put(attachmentNameArg,s"log_${jobRunID}.txt")
    inputMap.put(currentJobNameArg,subJobNames(4))
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    println("Executed start entry")
    try {
      mainSenderWithAttachment(inputMap)
      inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
      execSparkSql(spark,inputMap)
    }
    catch
      {
        case e:Exception=> {inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          println(e.printStackTrace)
          println("Failed while sending mail")
         System.exit(9)
        }
      }
  }
}
