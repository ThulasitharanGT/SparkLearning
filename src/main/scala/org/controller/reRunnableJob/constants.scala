package  org.controller.reRunnableJob

import org.apache.spark.sql.SaveMode

object constants
{
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
  val pwdArg="pwd"
  //basic
  val stringTrue="true"
  val stringFalse="false"
  val matchingString="Matching"
  val notMatchingString="Not-Matching"
  val failedMessage="Failed"
  val successMessage="Success"
  val runningString="RUNNING"
  val completedString="COMPLETED"
  val failedString="FAILED"

  //environment
  val hdfsURI="hdfs://"
  val prodConstant="prod"
  val devConstant="dev"

  // error messages
  val failedDueToLowMemoryMessage=s"${failedMessage} due to low memory"
  val readFromS3FailedMessage="Read from s3 failed due to Some Error. Please debug"
  val writeToHiveFailed="Data write to Hive table failed. Please debug "

  //delimiter
  val delimiterOr="|"
  val delimiterTilde="~"
  val delimiterComma=","

  //save Modes
  val appendMode=SaveMode.Append.toString.toLowerCase
  val overWriteMode=SaveMode.Overwrite.toString.toLowerCase

  //conf options
  val fsDefaultFS="fs.defaultFS"
  val hdfsDomain="hdfs.domain"
  //val subJobNames=Seq("RedshiftRead","temp_archival","HiveInsert","HiveSnapshotArchival")
  val subJobNames = Seq("wgetRead", "temp_archival", "HiveInsert", "HiveSnapshotArchival", "mailStatus")
  //mail options
  val fromEmailProperty="from.email"
  val fromPasswordProperty="from.pwd"
  val toEmailProperty="to.email"
  val outlookSMTPHost="smtp-mail.outlook.com"
  val mailSMPTHostProp="mail.smtp.host"
  val mailSMTPAuthProp="mail.smtp.auth"
  val mailSMPTStarttlsEnableProp="mail.smtp.starttls.enable"
  val mailSMTPPortProp="mail.smtp.port"
  val outlookSMTPPort="587"
  val smtpHost="smtp"
  val messageSubjectArg="messageSubject"
  val filePathArg="filePath"
  val attachmentNameArg="attachmentName"
  val messageContentArg="messageContent"
}