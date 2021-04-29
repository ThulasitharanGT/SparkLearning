package org.controller.deltaLakeEG

import org.apache.spark.sql.SparkSession
import org.util.SparkOpener
import org.util.readWriteUtil.{readDF, readStreamFunction, writeDF, writeStreamConsole}
import org.constants.projectConstants._
import scala.util.Try
import java.sql.DriverManager
import java.text.SimpleDateFormat
object readFromDeltaStreamAndPersistInTable extends SparkOpener{
  case class deltaCheck(strValue:String,intValue:Int,dateValue:java.sql.Date)
  case class deltaCheckWithStatus(strValue:String,intValue:Int,dateValue:java.sql.Date,intValueIncoming:Int)

  val spark=SparkSessionLoc()
  /*create table testPersist.delta_persist(
int_value INTEGER,
str_value varchar(20),
date_value date,
last_updated_timestamp timestamp default current_timestamp
);
*/
  spark.sparkContext.setLogLevel("ERROR")
  val simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd")

  def main(args:Array[String]):Unit={
  val inputMap=collection.mutable.Map[String,String]()
  for(arg <- args)
  {
    val keyPart=arg.split("=",2)(0)
    val valPart=arg.split("=",2)(1)
    inputMap.put(keyPart,valPart)
  }
    inputMap.put(fileFormatArg,deltaFormat)
    inputMap.put( filePathArgValue,inputMap("deltaInputPath"))
    val readStreamDF=readStreamFunction(spark,inputMap)
    inputMap.put(fileFormatArg,consoleFormat)
    inputMap.put(outputModeArg,fileAppendValue)
    inputMap.put(checkPointLocationArg,inputMap("checkpointLocation"))

  inputMap.put(driverOption,inputMap("mysqlDriver"))
  inputMap.put(userOption,inputMap("userName"))
  inputMap.put(passwordOption,inputMap("password"))
  inputMap.put(urlOption,inputMap("jdbcURL"))
  inputMap.put(dbtableWriteOption,s"${inputMap("databaseName")}.${inputMap("tableName")}")
  inputMap.put(fileTypeArgConstant,fileTypeJDBCValue)
  inputMap.put(fileOverwriteAppendArg,fileAppendValue)
import spark.implicits._
  writeStreamConsole(spark,inputMap,readStreamDF.coalesce(1)).foreachBatch((batchDF:org.apache.spark.sql.DataFrame,batchId:Long) =>{
 //   println(s"dbTable option inside for each batch ${inputMap(dbtableOption)}")
  //  batchDF.withColumn("batchDF_batchId",org.apache.spark.sql.functions.lit(batchId)).show(false)
    val computedRecords= batchDF.map(getStatusOfRecords(_,inputMap,spark))
    computedRecords.withColumn("computedRecords",org.apache.spark.sql.functions.lit(batchId)).printSchema
    val insertRecords=computedRecords.filter(_.intValue == -1).drop("intValue").selectExpr("strValue","intValueIncoming as intValue","dateValue")
    val updateRecords=computedRecords.filter(x => x.intValue != -1 && x.intValue != x.intValueIncoming)
    val nonUpdateRecords=computedRecords.filter(x => x.intValue == -1 && x.intValue == x.intValueIncoming)  // same value coming in so no changes are required
    insertRecords.show(false)
    updateRecords.show(false)
    nonUpdateRecords.show(false)
   // println(s"dbTable option inside for each batch before write ${inputMap(dbtableOption)}")
    writeDF(inputMap,insertRecords.selectExpr("strValue as str_value","intValue as int_value","dateValue as date_value"))
    updateRecords.map(updateRecord(_,inputMap)).withColumn("updateRecords",org.apache.spark.sql.functions.lit(batchId)).show(false)
    nonUpdateRecords.withColumn("nonUpdateRecords",org.apache.spark.sql.functions.lit(batchId)).show(false)

  }).start
    spark.streams.awaitAnyTermination
  }

  def updateRecord(recordWithInfo:deltaCheckWithStatus,inputMap:collection.mutable.Map[String,String])={
    val updateQuery=s"update ${inputMap(dbtableWriteOption)} set int_value='${recordWithInfo.intValueIncoming}',last_updated_timestamp=current_timestamp() where str_value='${recordWithInfo.strValue}' and date_value='${recordWithInfo.dateValue}'"
    Class.forName(inputMap(urlOption))
    val props=new java.util.Properties()
    props.put("url",inputMap(urlOption))
    props.put("user",inputMap(userOption))
    props.put("password",inputMap(passwordOption))
    val connection=DriverManager.getConnection(inputMap(urlOption),props)
    val updateStatement=connection.prepareStatement(updateQuery)
    val numAffectedRecords=Try{updateStatement.executeUpdate} match {
      case value if value.isSuccess == true => value.get
      case value if value.isFailure == true => -1
    }
    numAffectedRecords match {
      case value if value == -1=> (recordWithInfo,false)
      case _ => (recordWithInfo,true)
    }
  }
  def getStatusOfRecords(incomingRow:org.apache.spark.sql.Row,inputMap:collection.mutable.Map[String,String],spark:SparkSession)= {
  //  import spark.implicits._
 //   val record = deltaCheck(incomingRow.getString(0),incomingRow.getInt(1),incomingRow.getDate(2))
  //  println(s"dbTable option inside getExistingRecord before replacing ${inputMap(dbtableOption)}")
    println(s"getStatusOfRecords :: incomingRow ${incomingRow}")
    val selectQuery=s"(select str_value,int_value,date_value from ${inputMap(dbtableWriteOption)} where str_value='${incomingRow.getString(0)}' and date_value='${incomingRow.getString(2)}')a"
    println(s"selectQuery ${selectQuery}")
/*    Class.forName(inputMap(urlOption))
    val props=new java.util.Properties()
    props.put("url",inputMap(urlOption))
    props.put("user",inputMap(userOption))
    props.put("password",inputMap(passwordOption))
    val connection=DriverManager.getConnection(inputMap(urlOption),props)
    val selectStatement=connection.prepareStatement(selectQuery)*/
    inputMap.put(dbtableReadOption,selectQuery)
  //  println(s"dbTable option inside getExistingRecord after replacing ${inputMap(dbtableOption)}")
    val existingRecordDf=readDF(spark,inputMap) //
  //  Try this ===========>  spark.read.format("jdbc").option("driver","com.mysql.cj.jdbc.Driver").option("user","raptor").option("password","").option("url","jdbc:mysql://localhost:3306/testPersist").option("dbtable","(select str_value,int_value,date_value from testPersist.delta_persist where str_value='str1' and date_value='2020-02-01')a").load
    // if DF causes issue go the JDBC way
    println("DB read complete")
   // existingRecordDf.show(false)
    val recordPresent=Try{existingRecordDf.count} match {
      case value if value.isSuccess == true =>  // understanding is if record is present only one must be present
        println(s"read DF for incoming record is not null")
        value.get match {
          case value if value > 0L=>
            println(s"read DF contains records")
            val existingRecord=existingRecordDf.collect.toList(0)
            deltaCheckWithStatus(existingRecord.getString(0),existingRecord.getString(1).toInt,javaUtilDateToSqlDateConversion(existingRecord.getString(2)),incomingRow.getString(1).toInt)
          case _ =>
            println(s"read DF does not contain records")
            deltaCheckWithStatus(incomingRow.getString(0),-1,javaUtilDateToSqlDateConversion(incomingRow.getString(2)),incomingRow.getString(1).toInt) // if no record -1 in existing in value
        }
        // new java.sql.Date(simpleDateFormat.parse(existingRecord.getString(2)).getTime)
      case value if value.isFailure == true  =>
        println(s"read DF for incoming record is null")
        deltaCheckWithStatus(incomingRow.getString(0),-1,javaUtilDateToSqlDateConversion(incomingRow.getString(2)),incomingRow.getString(1).toInt) // if no record -1 in existing in value
    }
    println(s"getStatusOfRecords :: recordPresent ${recordPresent}")
    recordPresent
  }
// val dateString="2020-09-08"
  def javaUtilDateToSqlDateConversion(dateString:String)=   new java.sql.Date(simpleDateFormat.parse(dateString).getTime)

  //spark-submit --class org.controller.deltaLakeEG.readFromDeltaStreamAndPersistInTable --packages io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --driver-memory 512m --driver-cores 1 --num-executors 1 --executor-memory 512m --executor-cores 1  --master local[*] --conf spark.dynamicAllocation.enabled=false /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServers=localhost:9092,localhost:9093,localhost:9094 topic=deltaCheckTopic checkpointLocation=hdfs://localhost:8020/user/raptor/streams/deltaReadStream/ deltaInputPath=hdfs://localhost:8020/user/raptor/persist/deltaPersistPath/ startingOffsets=latest mysqlDriver="com.mysql.cj.jdbc.Driver" userName=raptor password= jdbcURL="jdbc:mysql://localhost:3306/testPersist" databaseName=testPersist tableName=delta_persist

/*
* Primarily taking int value as delta & data and string value as primary key
* */

}

