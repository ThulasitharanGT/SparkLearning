package org.controller.ingestionAndDQC

import org.apache.spark.sql.functions._
import java.util.Date


import org.controller.ingestionAndDQC.projectConstants._
import org.controller.ingestionAndDQC.readWriteUtil._

object readFromKafkaAndSaveToBronze extends sparkOpener {
  def main(args:Array[String]):Unit ={
    val spark=sparkSessionOpen()
    spark.sparkContext.setLogLevel("ERROR")
    val inputMap=collection.mutable.Map[String,String]()
    for (arg<- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
    val kafkaTopic=inputMap("topic")
    val checkpointFolderNameRead=inputMap("checkpointFolderNameRead")
    val checkpointFolderNameWrite=inputMap("checkpointFolderNameWrite")
    val jobRunId=jobRunIDDateFormat.format(new Date)
    val jobRunDate=dateFormat.format(new Date)
    val logFilePath=s"${kafkaToBronzeLogPath}${jobRunId}.log"
    var writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Starting ${jobNameStream} for ${jobRunDate}\n")
    writerObject.writeBytes(s"Entering entry in audit table for ${jobNameStream} for ${jobRunDate}\n")
    writerObject.close
    val bronzeFinalSelectExpr=Seq("key","topic","partition","offset","timestamp","timestampType","value[0] as reading","value[1] as year","value[2] as circuit","value[3] as session","value[4] as processDate")
    inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameStream}',current_timestamp(),'${statusStarted}')")//cast(${timeStampDateFormat.format(new Date)} as timestamp)
    execSparkSql(spark,inputMap)
    writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
    writerObject.writeBytes(s"Entered entry in audit table for ${jobNameStream} for ${jobRunDate}\n")
    writerObject.close
    try
      {
        import org.apache.spark.sql.types._
        inputMap.put(fileFormatArg,kafkaFormat)
        inputMap.put(bootstrapServersArg,kafkaBrokers)
        inputMap.put(subscribeArg,kafkaTopic)
        inputMap.put(keyDeserializerArg,kafkaStringDeserializerClass)
        inputMap.put(valueDeserializerArg,kafkaStringDeserializerClass)
        inputMap.put(startingOffsetsArg,startingOffsetsEarliest)
        inputMap.put(checkpointLocationArg,s"${checkpointBasePath}${checkpointFolderNameRead}/")
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Creating readStream from kafka topic ${kafkaTopic} from broker ${kafkaBrokers}\n")
        writerObject.close
        val readStreamDF= readStreamFunction(spark,inputMap).withColumn("key",lit(col("key").cast(StringType))).withColumn("value",lit(split(col("value").cast(StringType),"~"))).selectExpr(bronzeFinalSelectExpr:_*)
        writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
        writerObject.writeBytes(s"Registered readStream from kafka topic ${kafkaTopic} from broker ${kafkaBrokers}\n")
        writerObject.close
        // writing it to bronze
          readStreamDF.writeStream.option(checkpointLocationArg,s"${checkpointBasePath}${checkpointFolderNameWrite}/").foreachBatch{
          (batchDF:org.apache.spark.sql.DataFrame,batchProcessCounter:Long) =>
            val streamLogFilePath=s"${kafkaToBronzeLogPath}${jobRunId}_${dateNonHyphenFormat.format(new Date)}.log"
            val numOfRecordsInThisBatch=batchDF.count
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,streamLogFilePath)
            writerObject.writeBytes(s"Entering entry for start of ${jobNameStream} - ${batchProcessCounter} in audit table\n")
            writerObject.close
            inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameStream} - ${batchProcessCounter}',current_timestamp(),'${statusStarted}')")//cast(${timeStampDateFormat.format(new Date)} as timestamp)
            execSparkSql(spark,inputMap)
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,streamLogFilePath)
            writerObject.writeBytes(s"Entering entered for ${jobNameStream} - ${batchProcessCounter} in audit table\n")
            writerObject.close
            println(s"Read for iteration ${batchProcessCounter}. Records in this batch ${numOfRecordsInThisBatch}")
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,streamLogFilePath)
            writerObject.writeBytes(s"Read ${numOfRecordsInThisBatch} records in this batch\n")
            writerObject.close
            batchDF.withColumn("processedTimeStamp",lit(timeStampDateFormat.format(new Date))).withColumn("batchCounter",lit(batchProcessCounter)).write.format(deltaFileFormat).mode(appendMode).partitionBy("processDate").save(bronzeBasePath)
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,streamLogFilePath)
            writerObject.writeBytes(s"processed ${numOfRecordsInThisBatch} records in this batch\n")
            writerObject.close
            println(s"Written iteration ${batchProcessCounter}. Records processed this batch ${numOfRecordsInThisBatch}")
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,streamLogFilePath)
            writerObject.writeBytes(s"Entering entry for finish of ${jobNameStream} - ${batchProcessCounter} in audit table\n")
            writerObject.close
            inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameStream} - ${batchProcessCounter}',current_timestamp(),'${statusFinished}')")//'cast(${timeStampDateFormat.format(new Date)} as timestamp)'
            execSparkSql(spark,inputMap)
            writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,streamLogFilePath)
            writerObject.writeBytes(s"Entry entered for finish of ${jobNameStream} - ${batchProcessCounter} in audit table\n")
            writerObject.close
        }.start.awaitTermination
      }
    catch
      {
        case e:Exception => {
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
          writerObject.writeBytes(s"Failed ${jobNameStream} in ${dateFormat.format(new Date)}")
          writerObject.writeBytes(s"Entering entry for failure of ${jobNameStream} in audit table\n")
          writerObject.close
          inputMap.put(sqlStringArg,s"insert into ${auditTable} partition(job_run_date='${jobRunDate}') values(${jobRunId},'${jobName}','${jobNameStream}',current_timestamp(),'${statusFailure}')")//cast(${timeStampDateFormat.format(new Date)} as timestamp)
          execSparkSql(spark,inputMap)
          writerObject=fileOutputStreamObjectCreator(hdfsDomainLocal,logFilePath)
          writerObject.writeBytes(s"Entered entry for failure of ${jobNameStream} in audit table\n")
          writerObject.writeBytes(s"Stack Trace: \n${e.printStackTrace}")
          writerObject.close
          println(e.printStackTrace)
        }
      }

  }

}
