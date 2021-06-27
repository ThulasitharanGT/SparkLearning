package org.controller.persistingInsideJob

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.SparkOpener

import java.sql.{DriverManager,Connection,Timestamp}
import java.util.Properties
import com.typesafe.scalalogging.LazyLogging

object persistOutsideJob extends SparkOpener with LazyLogging{
  val spark=SparkSessionLoc()
  import spark.implicits._
  val parentKey="user_record"
  val childKey="bill_record"
  val commonSchema=new StructType(Array(StructField("mainTable",StringType,true),StructField("dataString",StringType,true),StructField("messageTime",StringType,true)))
  val parentSchema=new StructType(Array(StructField("name",StringType,true),StructField("id",StringType,true)))
  val childSchema=new StructType(Array(StructField("user_id",StringType,true),StructField("bill_id",StringType,true)))
  val minutesInAHour=60
  val secondsInAMinute=60
  val milliSecondInASecond=1000
  val hoursInADay=24
  def main(args:Array[String]):Unit ={
   val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    val parentTable=dbRead(inputMap,s"${inputMap("mysqlSchema")}.${inputMap("parentTable")}")

    spark.readStream.format("kafka").option("bootstrap.servers",s"${inputMap("bootstrapServers")}")
      .option("subscribe",s"${inputMap("inputTopic")}")
      .option("offsets",s"${inputMap("startingOffset")}").load.selectExpr("cast (value as string) value")
      .select(from_json($"value",commonSchema).as("dataFlattened")).selectExpr("dataFlattened.*")
      .writeStream.format("console").option("checkpointLocation",s"${inputMap("checkpointDir")}")
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)=> {
        forEachBatchFun(df,batchId,inputMap,parentTable)
    }).start

    /*
    startingOffset
    bootstrapServers
    inputTopic
    checkpointDir
mysqlJDBCDriver
mysqlJDBCUrl
mysqlUser
mysqlPassword
mysqlJDBCUrl
mysqlSchema
stateTable
parentTable
childTable
   */


  }

  def forEachBatchFun(df:org.apache.spark.sql.DataFrame,batchId:Long,inputMap: collection.mutable.Map[String,String],parentTableDF:org.apache.spark.sql.DataFrame)={
   logger.info(s"batchID - ${batchId}")
    val parentRecord=df.filter(s"mainTable = '${parentKey}'")
    val childRecord=df.filter(s"mainTable = '${childKey}'")
    val childRecordsTransformed=childRecord.select(col("messageTime"),col("dataString"),from_json(col("dataString"),childSchema).as("dataFlattened"))
      .selectExpr("messageTime","dataString","dataFlattened.*")
    val parentRecordsTransformed=parentRecord.select(col("messageTime"),from_json(col("dataString"),parentSchema).as("dataFlattened"))
      .selectExpr("messageTime","dataFlattened.*")

    // delete the expired records in state
    cleanUpExpiredState(inputMap)

    // Take the latest record from sate by message timestamp (query)
//spark.read.format("jdbc").option("driver","").option("url","").option("user","").option("password","").option("dbtable","")
    val queryForTakingLatestStateRecords=s"select message_time,resolve_key,message,dense_rank over (partition by resolve_key order by message_time desc) as ageCol from  ${inputMap("mysqlSchema")}.${inputMap("childTable")} where ageCol =1"
    logger.info(s"queryForTakingLatestStateRecords - ${queryForTakingLatestStateRecords}")
    val childState=dbRead(inputMap,s"${queryForTakingLatestStateRecords}").select(col("message_time"),col("message"),from_json(col("message"),childSchema).as("messageFlattened")).selectExpr("message_time as messageTime","message  as dataString","messageFlattened.*")


 /*
   val stateVsParentJoinDF=childState.as("state").join(parentRecordsTransformed.as("parent"),$"resolve_key"===$"id","left")
    val releasedChildStateDF=stateVsParentJoinDF.where("parent.id is not null").select("state.*").select(from_json(col("dataString"),childSchema).as("dataFlattened")).selectExpr("dataFlattened.*")
    val updatedState=stateVsParentJoinDF.where("parent.id is not null").select("state.*")
*/
    // going to do both incoming and DB for state and incoming release
    // both sate child and incoming child is looked up against incoming parent and db parent

    val totalParentDF=parentTableDF.select("name","id").as("dbParent").union(parentRecordsTransformed.select("name","id").as("inComingParent"))

    val totalChild= childRecordsTransformed.as("incomingChild").union(childState.as("stateChild"))

    val releaseJoinDF=totalParentDF.as("totalParent").join(totalChild.as("totalChild"),col("totalChild.user_id")===col("totalParent.id"),"right")
    val releasedDF=releaseJoinDF.where(s"totalParent.id is not null")
    val unReleasedDF=releaseJoinDF.where(s"totalParent.id is null")

    val transformedReleasedDF=releasedDF.select("totalChild.*").select("user_id","bill_id") // persist to original table
    val transformedUnreleasedDF=unReleasedDF.select("totalChild.*").selectExpr("messageTime as message_time","user_id as resolve_key","dataString as message")

  //  transformedUnreleasedDF.write.mode("overwrite").format("jdbc").option("driver","").option("url","").option("user","").option("password","").option("dbtable","").save

  //  transformedReleasedDF.write.mode("append").format("jdbc").option("driver","").option("url","").option("user","").option("password","").option("dbtable","").save

 //   parentRecordsTransformed.write.mode("append").format("jdbc").option("driver","").option("url","").option("user","").option("password","").option("dbtable","").save
    logger.info(s"Writing to state")
    dbSave(transformedUnreleasedDF,inputMap,"overwrite",s"${inputMap("mysqlSchema")}.${inputMap("stateTable")}")
    logger.info(s"Writing to child")
    dbSave(transformedReleasedDF,inputMap,"append",s"${inputMap("mysqlSchema")}.${inputMap("childTable")}")
    logger.info(s"Writing to parent")
    dbSave(parentRecordsTransformed,inputMap,"append",s"${inputMap("mysqlSchema")}.${inputMap("parentTable")}")

  }

  def getConnection(inputMap:collection.mutable.Map[String,String])={
    Class.forName(inputMap("mysqlJDBCDriver"))
    DriverManager.getConnection(inputMap("mysqlJDBCUrl"),getProps(inputMap))
  }
  def getProps(inputMap:collection.mutable.Map[String,String])={
    val props=new Properties
    props.put("user",inputMap("mysqlUser"))
    props.put("password",inputMap("mysqlPassword"))
    props.put("url",inputMap("mysqlJDBCUrl"))
    props
  }
  def cleanUpExpiredState(inputMap:collection.mutable.Map[String,String])={
  //  val expiryLimit=inputMap("stateExpiry") // expected to be given in hrs or mins or days
    val targetTime= new Timestamp(System.currentTimeMillis- hrsOrDaysOrMinsToMilliSecondsConverter(inputMap("stateExpiry")))
    val query=s"delete from ${inputMap("mysqlSchema")}.${inputMap("stateTable")} where message_time <= '${targetTime}'"
    logger.info(s"cleanupQuery ${query}")
    val connection=getConnection(inputMap)
    val preparedStatement=connection.prepareStatement(query)
    preparedStatement.executeUpdate match {
      case value =>
        connection.close
        value
    }
  }

  def hrsOrDaysOrMinsToMilliSecondsConverter(expiryString:String) =expiryString match {
    case value if value.contains("hrs") =>
      val expiryHours=expiryString.trim.split(" ")(0).toInt
      expiryHours*minutesInAHour*secondsInAMinute*milliSecondInASecond
    case value if value.contains("mins") =>
      val expiryMins=expiryString.trim.split(" ")(0).toInt
      expiryMins*secondsInAMinute*milliSecondInASecond
    case value if value.contains("days") =>
      val expiryDays=expiryString.trim.split(" ")(0).toInt
      expiryDays*hoursInADay*minutesInAHour*secondsInAMinute*milliSecondInASecond
    case _ =>
      0
  }

  def dbRead(inputMap:collection.mutable.Map[String,String],tableName:String)=spark.read.format("jdbc").option("driver",inputMap("mysqlJDBCDriver")).option("url",inputMap("mysqlJDBCUrl")).option("user",inputMap("mysqlUser")).option("password",inputMap("mysqlPassword")).option("dbtable",s"${tableName}").load
  def dbSave(df:org.apache.spark.sql.DataFrame,inputMap:collection.mutable.Map[String,String],outputMode:String,tableName:String)=df.write.format("jdbc").mode(s"${outputMode}").option("driver",inputMap("mysqlJDBCDriver")).option("url",inputMap("mysqlJDBCUrl")).option("user",inputMap("mysqlUser")).option("password",inputMap("mysqlPassword")).option("dbtable",s"${tableName}").save
}
