package org.controller.ingestionAndDQC

import java.text.SimpleDateFormat
import org.apache.spark.sql.SaveMode

object projectConstants {
  /*
  Hive Tables
  create database data_pipeline;

  create external table data_pipeline.job_audit_data_pipeline(
  job_run_id bigint,
  job_name string,
  job_sub_name string,
  job_status_entry_time timestamp,
  job_status string
  )
  partitioned by
  (job_run_date date)
  stored as PARQUET
  location
  'hdfs:///user/raptor/hadoop/dataProject/data_pipeline/external/tables/audit_table/';
 path without the folder tables between table_name and external has folders which belong to hive 1.2.0 table
  create table data_pipeline.job_audit_stats_table(
  job_run_id bigint,
  job_name string,
  job_sub_name string,
  job_status_entry_time timestamp,
  job_status string,
  data_date date,
  previous_day_Count_bronze bigint,
  job_Run_day_Count_bronze bigint,
  job_Run_day_Count_silver bigint,
  difference_job_Run_day_bronze_vs_silver bigint,
  difference_prev_day_bronze_vs_curr_day_bronze bigint,
    comment string
  )
  partitioned by
  (job_run_date date)
  stored as PARQUET
  location
  'hdfs:///user/raptor/hadoop/dataProject/data_pipeline/external/tables/bronze_vs_silver_stats/';
   */
  val timeStampDateFormat= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss")
  val dateNonHyphenFormat= new SimpleDateFormat("yyyyMMdd")
  val dateFormat= new SimpleDateFormat("yyyy-MM-dd")
  val jobRunIDDateFormat=new SimpleDateFormat("yyyyMMddHHmmsssss")

  val jobName="data collector for IOT"
  val jobNameStream="kafka to bronze"
  val jobNameBatch="bronze to silver"
  val jobNameStatsBatch="bronze vs silver vs previous day bronze check"
  val jobNameStatsMail="Status mail"
  val jobNameFixExtraRecordsInBronzeIntoSilver="Ingesting extra records from bronze to silver"
  val statusStarted="Started"
  val statusRunning="Running"
  val statusFailure="Failed"
  val statusFinished="Finished"
  val statusSuccess="Success"

  val kafkaFormat="kafka"
  val bootstrapServersArg="kafka.bootstrap.servers"
  val subscribeArg="subscribe"
  val keyConstant="key"
  val valueConstant="value"
  val serializerConstant="serializer"
  val deSerializerConstant="deserializer"
  val keyDeserializerArg=s"${keyConstant}.${deSerializerConstant}"
  val valueDeserializerArg=s"${valueConstant}.${deSerializerConstant}"
  val startingOffsetsArg="startingOffsets"
  val checkpointLocationArg="checkpointLocation"
  val sysCommandArg="sysCommand"
  val startingOffsetsEarliest="earliest"
  val startingOffsetsLatest="latest"

  val topic ="dataTopic"
  val kafkaBrokers="localhost:9091,localhost:9092,localhost:9093"
  val kafkaStringDeserializerClass="org.apache.kafka.common.serialization.StringDeserializer"
  val kafkaStringSerializerClass="org.apache.kafka.common.serialization.StringSerializer"

  val checkpointBasePath="/user/raptor/hadoop/dataProject/checkpoints/"
  val bronzeBasePath="/user/raptor/hadoop/dataProject/bronze/"
  val bronzeVsSilverStatsBasePath="/user/raptor/hadoop/dataProject/data_pipeline/external/tables/bronze_vs_silver_stats/"
  val silverBasePath="/user/raptor/hadoop/dataProject/silver/"
  val failureStatsTempFilePath="/user/raptor/hadoop/dataProject/fileSaveForMail/"
  val extraRecordsBronzeCatchupTempPathForCompaction="/user/raptor/hadoop/dataProject/bronzeCatchupTempForCompaction/"

  // log paths
  val logBasePath="/user/raptor/hadoop/dataProject/logs/"
  val kafkaToBronzeLogPath=s"${logBasePath}kafkaToBronzeLogs/"
  val bronzeToSilverLogPath=s"${logBasePath}bronzeToSilverLogs/"
  val statsJobLogPath=s"${logBasePath}statsJobLogs/"
  val statsMailJobLogPath=s"${logBasePath}statsMailLogs/"
  val fixExtraBronzeDataToSilverLogPath=s"${logBasePath}fixExtraBronzeDataToSilver/"

  val auditTableDB="data_pipeline"
  val auditTableName="job_audit_data_pipeline"
  val auditTable=s"${auditTableDB}.${auditTableName}"

  val statsTableDB="data_pipeline"
  val statsTableName="job_audit_stats_table"
  val statsTable=s"${statsTableDB}.${statsTableName}"

  val mailSubjectArg="mailSubject"
  val mailBodyArg="mailBody"
  val mailAttachmentPathArg="mailAttachmentPath"
  val mailAttachmentFileNameArg="mailAttachmentFileName"
  val fromMailIdArg="fromMailId"
  val toMailIdsArg="toMailIds"
  val fromMailIdPwdArg="fromMailIdPwd"
  val outlookSMTPHost="smtp-mail.outlook.com"
  val mailSMPTHostProp="mail.smtp.host"
  val mailSMTPAuthProp="mail.smtp.auth"
  val mailSMPTStarttlsEnableProp="mail.smtp.starttls.enable"
  val mailSMTPPortProp="mail.smtp.port"
  val outlookSMTPPort="587"
  val smtpHost="smtp"
  val hdfsDomain="hdfs.domain"
  val stringTrue="true"
  val hdfsDomainLocal="hdfs://localhost:8020/"

  // for new methods

  //property
  val redshiftDriverProperty="redshift.driver"
  val redshiftUrlProperty="redshift.url"
  val redshiftUserProperty="redshift.user"
  val redshiftPasswordProperty="redshift.pwd"
  val tablesListInputFileProperty="tablesList.path"
  val s3SecretKeyProperty="fs.s3a.awsSecretAccessKey"
  val s3AccessKeyProperty="fs.s3a.awsAccessKeyId"
  val sparkSerializerProperty="spark.serializer"
  val sparkMaxPartitionBytesProperty="spark.sql.files.maxPartitionBytes"
  val hiveTableBasePathProperty="hiveTable.basePath"
  val statsOutputPathProperty="statsOutput.path"
  val s3OutputPathProperty="s3Output.path"
  val inputFileOutputPath="inputFile.outputPath"

  //option
  val headerOption="header"
  val pathOption="path"
  val inferSchemaOption="inferSchema"
  val delimiterOption="delimiter"
  val driverOption="driver"
  val userOption="user"
  val passwordOption="password"
  val dbTableOption="dbtable"
  val urlOption="url"

  //format
  val csvFileFormatArg="csv"
  val dataBricksCSVformat="com.databricks.spark.csv"
  val jdbcFormat="jdbc"
  val jsonFileFormatArg="json"
  val parquetFileFormatArg="parquet"
  val deltaFileFormat="delta"
  //args
  val fileFormatArg="fileFormat"
  val coalesceArg="coalesce"
  val sqlStringArg="sqlString"
  val saveModeArg="saveMode"
  val tableNameArg="tableName"
  val DFrepartitionArg="DFrepartition"
  val dataDateArg="dataDate"
  val dataDatePathArg="%dataDate%"
  val tableNamePathArg="%table%"
  val logPathArg="logPath"
  val currentJobNameArg="currentJobName"
  val mainJobNameArg="mainJobName"
  val runDateArg="runDate"
  val jobRunIdArg="jobRunID"
  val wgetFileNameArg="wgetFileName"
  val wgetHttpPathArg="wgetHttpPath"

  //basic
  val stringFalse="false"
  val matchingString="Matching"
  val notMatchingString="Not-Matching"
  val failedMessage="Failed"
  val successMessage="Success"
  val runningString="RUNNING"
  val completedString="COMPLETED"
  val failedString="FAILED"

  //delimiter
  val delimiterOr="|"
  val delimiterTilde="~"
  val delimiterComma=","

  //save Modes
  val appendMode=SaveMode.Append.toString.toLowerCase
  val overWriteMode=SaveMode.Overwrite.toString.toLowerCase

  //conf options
  val fsDefaultFS="fs.defaultFS"
  val basePathArg="basePath"

  // conf
  val partitionByFlag="partitionByFlag"
  val partitionByColumns="partitionByColumns"
  val mailContentType="text/html; charset=utf-8"

}
