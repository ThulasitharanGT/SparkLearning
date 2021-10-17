package org.controller.rankingSystemF1

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.rankingSystemF1.utils.{getInputMap, getJDBCConnection, raceInfo, raceInfoWithResult, schemaOfOuterLayer, schemaOfRace, sendMessageToKafka}
import org.util.SparkOpener
import org.apache.spark.sql.expressions.Window

object raceInfoToTable extends SparkOpener {
  val spark = SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val inputMap = getInputMap(args)

    spark.readStream.format("kafka").option("subscribe",inputMap("topic"))
      .option("kafka.bootstrap.servers",inputMap("bootStrapServer")).option("startingOffsets",inputMap("startingOffsets"))
      .load
      .select(from_json(col("value").cast(StringType), schemaOfOuterLayer).as("valueStruct")).select("valueStruct.*")
      .filter("messageType ='raceInfo' ").drop("messageType")
      .select(from_json(col("incomingMessage").cast(StringType), schemaOfRace).as("valueStruct"), col("messageTimestamp")
        ).selectExpr("valueStruct.*", "cast(messageTimestamp as timestamp) messageTimestamp")
      .writeStream.format("console")
      .option("checkpointLocation",inputMap("checkPointLocation"))
      .foreachBatch((df: org.apache.spark.sql.DataFrame, batchId: Long) => {
        forEachBatchFunction(df, batchId, inputMap)
      }).start
    spark.streams.awaitAnyTermination
  }

  // race track id is parent

def forEachBatchFunction(df:org.apache.spark.sql.DataFrame,batchId:Long,inputMap:collection.mutable.Map[String,String])={

  // delete expired records
  val conn=getJDBCConnection(inputMap)
  deleteExpiredRecords(conn,"raceInfo",inputMap)
  // release race track id records
  // hold

 // df.show(false)
  val incomingRaceTrackIds=df.select("raceTrackId").collect.map(_(0).toString)

    val recordsInState=spark.read.format("jdbc") .option("driver",inputMap("JDBCDriver"))
     .option("user",inputMap("JDBCUser"))
     .option("password",inputMap("JDBCPassword"))
     .option("url",inputMap("JDBCUrl"))
     .option("dbtable",s"(select message_in_json,incoming_timestamp from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name='raceInfo')a").load
     .select(from_json(col("message_in_json"),schemaOfRace).as("raceExtract"),col("incoming_timestamp"))
     .selectExpr("raceExtract.*","incoming_timestamp as messageTimestamp")

 // recordsInState.withColumn("recordsInState",lit("recordsInState")).show(false)

  val recordsIncoming=df

  val raceTrackInfoIncoming=spark.read.format("jdbc").option("driver",inputMap("JDBCDriver"))
    .option("user",inputMap("JDBCUser"))
    .option("password",inputMap("JDBCPassword"))
    .option("url",inputMap("JDBCUrl"))
    .option("dbtable",s"(select * from ${inputMap("schemaName")}.${inputMap("raceTrackInfoTable")} where race_track_id in ('${incomingRaceTrackIds.mkString("','")}') )a").load

  val raceTrackInfoFromTemp=spark.read.format("jdbc").option("driver",inputMap("JDBCDriver"))
    .option("user",inputMap("JDBCUser"))
    .option("password",inputMap("JDBCPassword"))
    .option("url",inputMap("JDBCUrl"))
    .option("dbtable",s"(select * from ${inputMap("schemaName")}.${inputMap("raceTrackInfoTable")} where race_track_id in (select resolve_key from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name='raceInfo') )a").load

//  raceTrackInfoFromTemp.withColumn("raceTrackInfoFromTemp",lit("raceTrackInfoFromTemp")).show(false)
  val availableAndRequiredRaceTrackId=raceTrackInfoIncoming.union(raceTrackInfoFromTemp)

  val recordsInStateAndIncoming=recordsIncoming.union(recordsInState)
 // recordsInStateAndIncoming.withColumn("recordsInStateAndIncoming",lit("recordsInStateAndIncoming")).show(false)

  val recordsJoinForResult=recordsInStateAndIncoming.as("raceInfo").join(availableAndRequiredRaceTrackId.as("raceTrack"),
    trim(col("raceInfo.raceTrackId"))=== trim(col("raceTrack.race_track_id")),"left")

 // recordsJoinForResult.withColumn("recordsJoinForResult",lit("recordsJoinForResult")).show(false)

  val releasedRecords=recordsJoinForResult.filter("raceTrack.race_track_id is not null ").select("raceInfo.*")
  val holdRecords=recordsJoinForResult.filter("raceTrack.race_track_id is null ").select("raceInfo.*")

 // releasedRecords.withColumn("releasedRecords",lit("releasedRecords")).show(false)
 // holdRecords.withColumn("holdRecords",lit("holdRecords")).show(false)

  releasedRecords.withColumn("dupeCheck",row_number.over(Window.partitionBy("raceId","raceTrackId","raceDate","season")
    .orderBy(desc("messageTimestamp"))))
    .filter("dupeCheck=1").drop("dupeCheck").as[raceInfo].map(x => {
    val conn=getJDBCConnection(inputMap)
    deleteReleasedRaceRecords(conn,x,inputMap)
    val (resultType,rowsAffected)=insertOrUpdateRaceInfo(conn,x,inputMap)
    conn.close
    sendMessageToKafka(s"""{"messageType":"triggerDriverPoints","messageTimestamp":"${new java.sql.Timestamp(System.currentTimeMillis)}","incomingMessage":"${x.toKafkaPayload}"}""",inputMap)
    raceInfoWithResult(x.raceId,x.raceTrackId,x.raceDate,x.season,x.messageTimestamp,resultType,rowsAffected)
  }).show(false)

  holdRecords.withColumn("dupeCheck",row_number.over(Window.partitionBy("raceId","raceTrackId","raceDate","season")
    .orderBy(desc("messageTimestamp"))))
    .filter("dupeCheck=1").drop("dupeCheck").as[raceInfo].map(x => {
    val conn=getJDBCConnection(inputMap)
    val (resultType,rowsAffected)=insertOrUpdateTempRecords(conn,x,inputMap)
    conn.close
    raceInfoWithResult(x.raceId,x.raceTrackId,x.raceDate,x.season,x.messageTimestamp,resultType,rowsAffected)
  }).show(false)

}

  def deleteExpiredRecords(conn:java.sql.Connection,jobName:String,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"delete from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name='${jobName}' and incoming_timestamp <= '${new java.sql.Timestamp(System.currentTimeMillis - inputMap("stateExpiry").toLong)}'").executeUpdate

  def deleteReleasedRaceRecords(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"delete from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name='raceInfo' and resolve_key='${record.raceTrackId}'").executeUpdate

  def doesRaceInfoRecordExists(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("raceInfoTable")} where race_id='${record.raceId}' and race_track_id='${record.raceTrackId}'").executeQuery.next

  def updateRaceInfoRecord(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("raceInfoTable")} set incoming_timestamp='${record.messageTimestamp}' where race_id='${record.raceId}' and race_track_id='${record.raceTrackId}'").executeUpdate

  def insertRaceInfoRecord(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("raceInfoTable")}(race_id, race_track_id, race_date, race_season, incoming_timestamp) values('${record.raceId}','${record.raceTrackId}','${record.raceDate}','${record.season}','${record.messageTimestamp}')").executeUpdate

  def insertOrUpdateRaceInfo(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String])=doesRaceInfoRecordExists(conn, record, inputMap) match {
    case value if value == true=> ("update",updateRaceInfoRecord(conn, record, inputMap))
    case value if value == false=> ("insert",insertRaceInfoRecord(conn, record, inputMap))
  }

  def insertOrUpdateTempRecords(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String])=doesRaceTmpRecordExist(conn,record,inputMap) match {
    case value if value == true => ("update",updateRaceTmpRecord (conn,record,inputMap) )
    case value if value == false => ("insert",insertRaceTmpRecord (conn,record,inputMap))
  }


  def doesRaceTmpRecordExist(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where resolve_key='${record.raceTrackId}' and job_name='raceInfo' ").executeQuery.next

  def updateRaceTmpRecord(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} set incoming_timestamp='${record.messageTimestamp}' where resolve_key='${record.raceTrackId}' and job_name='raceInfo' ").executeUpdate

  def insertRaceTmpRecord(conn:java.sql.Connection,record:raceInfo,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("driverPointsTemp")}(resolve_key, job_name, incoming_timestamp, message_in_json) values ('${record.raceTrackId}','raceInfo','${record.messageTimestamp}','${record.toString}') ").executeUpdate


  //race_id varchar(10),
  //race_track_id varchar(10),
  //race_date date,
  //race_season integer,
  //incoming_timestamp
}
