package org.controller.persistingInsideJob

import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import jobConstantsPersist._
import java.sql.DriverManager
import java.util.Properties
import scala.util.Try
import jobConstantsPersist._
import jobHelper._

object stage1Job extends SparkOpener{

  def main(args:Array[String]):Unit ={
    val spark=SparkSessionLoc("")
    spark.sparkContext.setLogLevel("ERROR")
    val inputMap=collection.mutable.Map[String,String]()
    for(arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valPart=arg.split("=",2)(1)
        inputMap.put(keyPart,valPart)
      }
    val bootstrapServer=inputMap("bootstrapServer")
    val topic=inputMap("topic")
    val checkpointLocation=inputMap("checkpointLocation")
    val offsetForTopic=inputMap("offsetForTopic")
 // {teamName,"teamId",checkFlag}

    val schemaOfEvent=StructType(Array(StructField("teamName",StringType,true),StructField("teamId",StringType,true),StructField("checkFlag",StringType,true)))
    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",bootstrapServer).option("subscribe",topic).option("offset",offsetForTopic).load.select(from_json(col("value").cast(StringType),schemaOfMessage).as("eventConverted")).selectExpr("eventConverted.*").filter(s"eventInfo = '${teamEvent}'").select(from_json(col("eventData").cast(StringType),schemaOfEvent).as("valueTmp")).selectExpr("valueTmp.*")

    readStreamDF.writeStream.format("console").outputMode("append").foreachBatch(
      (batchDF:DataFrame,batchID:Long)=>
        performInvalidateAndInsert(batchDF,spark,inputMap)
    ).option("checkpointLocation",checkpointLocation).trigger(Trigger.ProcessingTime(10)).start

    spark.streams.awaitAnyTermination
  }
 // case class teamInfo(teamName:String,teamId:String,checkFlag:String)
  /*

create table testPersist.team_info(team_name VARCHAR(50),
team_id VARCHAR(50) ,
team_valid_flag VARCHAR(50) ,
insert_timestamp TIMESTAMP DEFAULT current_timestamp(),
delete_timestamp TIMESTAMP DEFAULT NULL);



team_name,team_id,team_valid_flag,delete_timestamp
   */

  def performInvalidateAndInsert(batchDF:DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String])={
    val incomingList=idGetter(batchDF,1)
    // println(s"Incoming list ${incomingList}")
   // batchDF.show(false)
    inputMap.put("whereCondition","and delete_timestamp is NULL")
    val idListForQuery=idListToStringManipulator(incomingList)
    inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("tableName")} where team_id in (${idListForQuery}) ${Try{inputMap("whereCondition")}.isSuccess match {case true => inputMap("whereCondition") case false =>""}}) a")
    val existingValidRecordsDF=getExistingRecords(inputMap,spark)
    //  inputMap.put("whereCondition","and delete_timestamp is not null")
    //   val existingInvalidRecordsDF=getExistingRecords(incomingList,inputMap,spark)
    val validRecordsIdList=idGetter(existingValidRecordsDF,1)
  //  existingValidRecordsDF.show(false)
  val idForUpdate=idListToStringManipulator(validRecordsIdList)
    inputMap.put("sqlQuery",s"update ${inputMap("databaseName")}.${inputMap("tableName")} set team_valid_flag='N',delete_timestamp= current_timestamp() where team_id in (${idForUpdate}) and delete_timestamp is null")
    updateExistingValidRecords(inputMap)
    insertingAllRecords(batchDF,inputMap)
  }


 /* def idGetter(df:DataFrame,spark:SparkSession):List[String]={
    import spark.implicits._
    var tmpArrayBuffer=collection.mutable.ArrayBuffer[String]()
    println(s"inside idGetter")
    // df.show(false)
    // need an action on a DF to make the map work
    df.collect.map(x => {tmpArrayBuffer += x(1).toString;/* println(s"ID in this Row list ${x(1).toString}") ;teamInfo(x.getString(0),x.getString(1),x.getString(2)) */})
  //  println(s"Incoming list ${tmpList}")
   // df.show(false)
   // println(s"inside idGetter tmpArrayBuffer - ${tmpArrayBuffer}")
    tmpArrayBuffer.toList.distinct
  }*/


/*

{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Redbull Racing\",\"teamId\":\"T001\",\"checkFlag\":\"Y\"}"}
{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Alpha Tauri\",\"teamId\":\"T002\",\"checkFlag\":\"Y\"}"}
{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Mclaren Racing\",\"teamId\":\"T003\",\"checkFlag\":\"Y\"}"}
{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Aston Martin Racing\",\"teamId\":\"T004\",\"checkFlag\":\"Y\"}"}
{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Alpine Racing\",\"teamId\":\"T005\",\"checkFlag\":\"Y\"}"}
{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Williams Racing\",\"teamId\":\"T008\",\"checkFlag\":\"Y\"}"}


// reads data and updates into reference table

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.23 --class org.controller.persistingInsideJob.stage1Job --num-executors 2 --executor-cores 2 --executor-memory 512m --driver-memory 512m --driver-cores 2 --deploy-mode client --master local[*] /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServer=localhost:9092,localhost:9093,localhost:9094 topic=teamInfoTopic checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tst1/ offsetForTopic=latest driverMYSQL="com.mysql.cj.jdbc.Driver" username=raptor password= urlJDBC="jdbc:mysql://localhost:3306/testPersist" databaseName=testPersist tableName=team_info*/


  def insertingAllRecords(batchDF:DataFrame,inputMap:collection.mutable.Map[String,String])= batchDF.select(col("teamName").as("team_name"),col("teamId").as("team_id"),col("checkFlag").as("team_valid_flag"))/*.withColumn("delete_timestamp",lit(null))*/.write.format("jdbc").mode("append").option("driver",inputMap("driverMYSQL")).option("user",inputMap("username")).option("password",inputMap("password")).option("url",inputMap("urlJDBC")).option("dbtable",s"${inputMap("databaseName")}.${inputMap("tableName")}").save

}

// jdbc:mysql://localhost:3306/databaseName