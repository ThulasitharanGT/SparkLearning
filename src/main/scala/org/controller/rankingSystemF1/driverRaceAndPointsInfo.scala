package org.controller.rankingSystemF1

// persist incoming record

// try state on race ID. raceId to be parent

import org.controller.rankingSystemF1.utils.{driverInfoWithReleaseInd, driverPointInfo, driverPointInfoWithResult, driverPointsInfo, driverRaceInfo, driverTmpTable, driverTmpTableWithResult, driverRaceInfoWithReleaseInd, driver_race_info, driver_race_infoWithResult, getInputMap, getJDBCConnection, schemaOfDriverPoints, schemaOfDriverRace, schemaOfOuterLayer, sendMessageToKafka}
import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object driverRaceAndPointsInfo extends SparkOpener{
  val spark=SparkSessionLoc()
/*

  def getNonZeroDriverRecordFromTable(incomingRecord:driverTier1,inputMap:collection.mutable.Map[String,String])={
    val selectQuery=s"select driver_id,race_id,driver_name,point,season,incoming_ts from ${inputMap("schemaName")}.${inputMap("driverPointsTable")} where driver_id='${incomingRecord.driverId}' and season='${incomingRecord.season}' and point!=0 order by incoming_ts desc"
    println(s"getLatestTsForNonZeroRecordFromTable :: selectQuery ${selectQuery}")
    getDriverRecords(getJDBCConnection(inputMap),selectQuery)
  }


  def getDriverRecords(conn:java.sql.Connection,selectQuery:String)={
    var recordSeq:Seq[driverTier1]=Seq.empty
    val rs=getResultSet(conn,selectQuery)
    while (rs.next)
    {
      val driverId=rs.getString(1)
      println(s"driverId = ${driverId}")
      val raceId=rs.getString(2)
      println(s"raceId = ${raceId}")
      val driverName=rs.getString(3)
      println(s"driverName = ${driverName}")
      val point=rs.getInt(4)
      println(s"point = ${point}")
      val season=rs.getInt(5)
      println(s"season = ${season}")
      val incomingTs= rs.getTimestamp(6)
      println(s"incomingTs = ${incomingTs}")
      recordSeq=recordSeq:+ driverTier1(driverId,raceId,driverName,point,season,incomingTs)
    }
    conn.close
    recordSeq
  }
*/


def main(args:Array[String]):Unit={
  val inputMap=getInputMap(args)

    spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootStrapServer"))
      .option("startingOffsets",inputMap("startingOffsets"))
      .option("subscribe",inputMap("topic")).load.select(col("value").cast(StringType).as("value"))
      .select(from_json(col("value"),schemaOfOuterLayer).as("valueConverted"))
      .select("valueConverted.*").writeStream.format("console").outputMode("append")
      .option("checkpointLocation",inputMap("checkPointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)=>{
        batchFunction(df,batchId,inputMap)
      }).start

  spark.streams.awaitAnyTermination
}
  def batchFunction(df:org.apache.spark.sql.DataFrame,batchID:Long,inputMap:collection.mutable.Map[String,String])={
    import spark.implicits._
    deleteExpiredRecords(getJDBCConnection(inputMap),inputMap)

    // incoming records

    val driverRaceRecords=df.filter("messageType ='driverRaceInfo'")
      .select(from_json(col("incomingMessage"),schemaOfDriverRace).as("valueExtracted")
        ,col("messageTimestamp").as("incomingTs"))
      .select("valueExtracted.*","incomingTs")

    val driverPointRecords=df.filter("messageType ='driverPointsInfo'")
      .select(from_json(col("incomingMessage"),schemaOfDriverPoints).as("valueExtracted")
        ,col("messageTimestamp").as("incomingTs"))
      .select("valueExtracted.*","incomingTs")


    // delete records which got expired

    // driver is the parent of driver race and driver race is parent of point info

    // reading relevant records from state

    val recordsFromDriverTemp=spark.read.format("jdbc")
      .option("driver",inputMap("dbDriver"))
      .option("user",inputMap("dbUser"))
      .option("password",inputMap("dbPassword"))
      .option("url",inputMap("dbURL"))
      .option("dbtable",s"select * from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name in ('driverRace','driverPoints')")
      .load

    // releasing driver records

    val totalDriverRaceRecords=driverRaceRecords.union(recordsFromDriverTemp.filter("job_name='driverRace'")
      .select(from_json(col("message_in_json"),schemaOfDriverRace).as("valueExtracted")
      ,col("incoming_timestamp").as("incomingTs"))
      .select("valueExtracted.*","incomingTs")).as[driverRaceInfo]
      .groupByKey(x => (x.raceId,x.driverId))
      .flatMapGroups((raceIDAndSeason,raceRecords) => {
        val conn=getJDBCConnection(inputMap)
        val releaseInd=raceIDCheckInTable(conn,raceIDAndSeason._1,inputMap) && driverIDCheckInTable(conn,raceIDAndSeason._2,inputMap)  match {
          case value if value == true => "release"
          case value if value == true => "hold"
        }
        conn.close
        raceRecords.toList.map(x=> driverRaceInfoWithReleaseInd(x.driverId,x.raceId,x.incomingTs,releaseInd))
      }).toDF


    // driver release and hold

    val totalDriverRaceReleased=totalDriverRaceRecords.filter("releaseInd= 'release'").drop("releaseInd") // insert into table then delete these records in tmp
    val totalDriverRaceHold=totalDriverRaceRecords.filter("releaseInd= 'hold'").drop("releaseInd") // insert into temp table if present update incoming ts

    // points check

    val totalDriverPointsRecords=driverPointRecords.union(recordsFromDriverTemp.filter("job_name='driverPoints'")
      .select(from_json(col("message_in_json"),schemaOfDriverPoints).as("valueExtracted")
      ,col("incoming_timestamp").as("messageTimestamp"))
      .select("valueExtracted.*","messageTimestamp"))

    val incomingDriverAndSeason=driverPointRecords.select("driverId","season").distinct.collect.map(x=> (x(0).toString,x(1).toString)).toList

    // reading incoming  driver id's and season for points
    val driverIdsFromTable=spark.read.format("jdbc")
      .option("driver",inputMap("dbDriver"))
      .option("user",inputMap("dbUser"))
      .option("password",inputMap("dbPassword"))
      .option("url",inputMap("dbURL"))
      .option("dbtable",s"(select * from ${inputMap("schemaName")}.${inputMap("driverInfoTable")} where driver_id in ('${incomingDriverAndSeason.map(_._1).mkString("','")}') and season in ('${incomingDriverAndSeason.map(_._1).mkString("','")}') )")
      .load.selectExpr("driver_id as driverId","season")

    // driver id's released and from table

    val totalDriverIds=totalDriverRaceReleased.select("driverId","season").union(driverIdsFromTable)

    val driverPointsJoinForRelease=totalDriverPointsRecords.as("driverPoints").join(totalDriverIds.as("driverIds"),col("driverPoints.driverId")===col("driverIds.driverId") && col("driverPoints.season")===col("driverIds.season"),"left")

    // released points

    val driverPointsReleased=driverPointsJoinForRelease.filter("driverIds.driverId is not null").select("driverPoints.*")

    val driverPointsHold=driverPointsJoinForRelease.filter("driverIds.driverId is null").select("driverPoints.*")

    // save to table
    totalDriverRaceReleased.selectExpr("driverId as driver_id","raceId as race_entry","cast(messageTimestamp as timestamp) messageTimestamp")
      .withColumn("rankFilter",
        row_number.over(Window.partitionBy("driver_id","race_entry").orderBy(desc("messageTimestamp"))))
      .where("rankFilter=1").drop("rankFilter").as[driver_race_info].map(
      x => {
        val conn= getJDBCConnection(inputMap)
        deleteDriverTmpRecords(conn,"driverRace",x.race_id,inputMap) // deleting released records
         val (resultType,rowsAffected) =doesDriverRaceRecordExist(conn,x,inputMap)  match {
          case value if value == true =>("update",updateDriverRaceTimeStamp(conn,x,inputMap))
          case value if value == false =>  ("insert",insertDriverRaceTimeStamp(conn,x,inputMap))
        }
        conn.close
        driver_race_infoWithResult(x.driver_id,x.race_id,x.messageTimestamp,resultType,rowsAffected)
      }
    ).show(false)


    driverPointsReleased.selectExpr("driverId as driver_id","raceId as race_id","position","season","point","messageTimestamp")
      .withColumn("dupeFilter",
        row_number.over(Window.partitionBy("driver_id","race_id","position").orderBy(desc("messageTimestamp"))))
      .filter("dupeFilter=1").drop("dupeFilter")
      .as[driverPointInfo]
      .map(x => {
        val conn=getJDBCConnection(inputMap)
        deleteDriverTmpRecords(conn,"driverPoints",s"${x.race_id}|${x.driver_id}",inputMap) // deleting released records
        val (resultType,rowsAffected)=doesDriverPointExists(conn,x,inputMap) match {
          case value if value ==true => ("update",updateDriverPointsTimeStamp(conn,x,inputMap))
          case value if value == false=> ("insert",insertDriverPointsTimeStamp(conn,x,inputMap))
        }
        conn.close
        driverPointInfoWithResult(x.driver_id,x.race_id,x.position,x.season,x.point,x.messageTimestamp,resultType,rowsAffected)
      }).groupByKey(_.season).flatMapGroups((x,y)=>{
      sendMessageToKafka(s"""{"messageType":"pointsRecalculation","incomingMessage":"{\\"season\\":\\"${x}\\"}" ,"messageTimestamp":"${new java.sql.Timestamp(System.currentTimeMillis)}""",inputMap)
      y.toList
    }).repartition(10).show(false)

// persist hold records
    totalDriverRaceHold.withColumn("rankFilter",
      row_number.over(Window.partitionBy("driverId","raceId").orderBy(desc("messageTimestamp"))))
      .where("rankFilter=1").drop("rankFilter")
      .as[driverRaceInfo].map(x => driverTmpTable(x.raceId,x.toString,x.incomingTs)).map(x =>
    {
      val conn=getJDBCConnection(inputMap)
      val (actType,rowsAffected)=insertOrUpdateTempTable(conn,x,"driverRace",inputMap)
      conn.close
      driverTmpTableWithResult(x.resolveKey,x.messageJson,x.incomingTs,actType,rowsAffected)
    }).show(false)


    driverPointsHold
      .withColumn("dupeFilter",
        row_number.over(Window.partitionBy("driverId","raceId","position").orderBy(desc("messageTimestamp"))))
      .filter("dupeFilter=1").drop("dupeFilter").as[driverPointsInfo]
      .map(x=> driverTmpTable(s"${x.raceId}|${x.driverId}",x.toString,x.messageTimestamp))
      .map (x => {
        val conn=getJDBCConnection(inputMap)
        val (actType,rowsAffected)=insertOrUpdateTempTable(conn,x,"driverPoints",inputMap)
        conn.close
        driverTmpTableWithResult(x.resolveKey,x.messageJson,x.incomingTs,actType,rowsAffected)
      }).show(false)



  }


  def deleteDriverTmpRecords(conn:java.sql.Connection,jobName:String,key:String,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"delete from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name ='${jobName}' and key='${key}' ").executeUpdate

  def insertDriverTmpRecords(conn:java.sql.Connection, record:driverTmpTable, jobName:String, inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("driverPointsTemp")}(resolve_key,job_name,incoming_timestamp,message_in_json) values ('${record.resolveKey}','${jobName}','${record.incomingTs}','${record.messageJson}') ").executeUpdate

  def insertOrUpdateTempTable(conn:java.sql.Connection, record:driverTmpTable,jobName:String, inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where resolve_key='${record.resolveKey}' and job_name='driverRace' ").executeQuery.next match {
      case value if value == true => ("update",updateDriverTmpRecords(conn,record.resolveKey,jobName,record.incomingTs,inputMap))
      case value if value == false => ("insert",insertDriverTmpRecords(conn,record,jobName,inputMap))
    }

  def updateDriverTmpRecords(conn:java.sql.Connection,key:String,jobName:String,timeStamp:java.sql.Timestamp,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} set incoming_timestamp='${timeStamp}' where key='${key}' and job_name='${jobName}' ").executeUpdate


  def updateDriverRaceTimeStamp(conn:java.sql.Connection,record:driver_race_info,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("driverRaceInfoTable")} set incoming_timestamp='${record.messageTimestamp}' where driver_id='${record.driver_id}' and race_id='${record.race_id}'").executeUpdate

  def insertDriverRaceTimeStamp(conn:java.sql.Connection,record:driver_race_info,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("driverRaceInfoTable")}(driver_id,race_id,incoming_timestamp) values('${record.driver_id}','${record.race_id}','${record.messageTimestamp}'").executeUpdate

  def doesDriverRaceRecordExist(conn:java.sql.Connection,record:driver_race_info,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("driverRaceInfoTable")} where driver_id='${record.driver_id}' and race_id='${record.race_id}' ").executeQuery.next


  def doesDriverPointExists(conn:java.sql.Connection,record:driverPointInfo,inputMap:collection.mutable.Map[String,String])=
  conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("driverPointsTable")} where driver_id='${record.driver_id}' and race_id='${record.race_id}' and point='${record.point}'").executeQuery.next

  def updateDriverPointsTimeStamp(conn:java.sql.Connection,record:driverPointInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("driverPointsTable")} set incoming_timestamp='${record.messageTimestamp}' where driver_id='${record.driver_id}' and race_id='${record.race_id}' and point='${record.point}'").executeUpdate

  def insertDriverPointsTimeStamp(conn:java.sql.Connection,record:driverPointInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("driverPointsTable")} values (driver_id,race_id,position,season,point,incoming_timestamp) values('${record.driver_id}','${record.race_id}','${record.position}','${record.season}','${record.point}','${record.messageTimestamp}')").executeUpdate

  def raceIDCheckInTable(connection:java.sql.Connection,raceID:String, inputMap:collection.mutable.Map[String,String])=
    connection.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("raceInfoTable")} where race_id='${raceID}'").executeQuery.next

  def driverIDCheckInTable(connection:java.sql.Connection,driverId:String, inputMap:collection.mutable.Map[String,String])=
    connection.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("driverInfoTable")} where driver_id='${driverId}'").executeQuery.next

  def deleteExpiredRecords(connection:java.sql.Connection,inputMap:collection.mutable.Map[String,String])= {
    connection.prepareStatement( s"delete from ${inputMap("schemaName")}.${inputMap("driverPointsTemp")} where job_name in ('driverInfo','driverPointsInfo') and incoming_timestamp <= '${new java.sql.Timestamp(System.currentTimeMillis -(inputMap("stateExpiry").toLong) )}'").executeUpdate
    connection.close
  }


}
