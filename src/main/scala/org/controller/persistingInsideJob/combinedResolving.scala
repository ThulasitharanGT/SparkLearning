package org.controller.persistingInsideJob

import org.util.SparkOpener
import jobHelper._
import jobConstantsPersist._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger

import scala.util.Try

object combinedResolving extends SparkOpener {
def main(args:Array[String]):Unit={
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
 // val inputMap=collection.mutable.Map[String,String]()
  val inputMap=argsToMapConvert(args)
  val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topics")).option("offset",inputMap("offsetForTopic")).load.select(from_json(col("value").cast(StringType),schemaOfMessage).as("convertedMessage")).selectExpr("convertedMessage.*")

  val teamStreamDF=readStreamDF.where(s"eventInfo='${teamEvent}'").select(from_json(col("eventData").cast(StringType),teamSchema).as("teamData")).selectExpr("teamData.*")
  val driverStreamDF=readStreamDF.where(s"eventInfo='${driverEvent}'").select(from_json(col("eventData").cast(StringType),driverSchema).as("driverData")).selectExpr("driverData.*")

  var driverPersistDFOut:DataFrame=null
  var teamPersistDFOut:DataFrame=null

  teamStreamDF.writeStream.format("console").outputMode("update").option("checkpointLocation",inputMap("teamStreamCheckpoint")).foreachBatch(
    (batchDF:DataFrame,batchID:Long)=>
      {
        var teamPersistDFIn=teamPersistDFOut
        var driverPersistDFInTeam=driverPersistDFOut
     //   batchDF.withColumn("batchDFTeam",lit("batchDFTeam")).show(false)

        val teamIDIncoming=idGetterTeam(batchDF) //idGetter(batchDF,1)  teamTableName
        inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("driverTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}) )a")
      //  val existingRecordsInDriverTable=getExistingRecords(inputMap,spark)
        val existingRecordsInDriverTable=getExistingRecords(inputMap,spark,s"(select * from ${inputMap("databaseName")}.${inputMap("driverTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}) )a")
   //     existingRecordsInDriverTable.withColumn("existingRecordsInDriverTableTeam",lit("existingRecordsInDriverTeam")).show(false)
        var existingDriverRecInStateAndDB:DataFrame=null

        // appending existing state driver with db driver records
        /*Try{driverPersistDFInTeam.count}.isSuccess */driverPersistDFInTeam match {
          case null =>
            println("No records in driver state team stream")
            existingDriverRecInStateAndDB=existingRecordsInDriverTable.drop("insert_timestamp","delete_timestamp")
        //    existingDriverRecInStateAndDB.withColumn("driverRecordsInDBTeamStream",lit("driverRecordsInDBTeamStream")).show(false)
          case _ =>
            println("Records in driver state team stream")
            existingDriverRecInStateAndDB=(driverPersistDFInTeam.drop("receivedTimeStamp","receivedBatch")).union(existingRecordsInDriverTable.drop("insert_timestamp","delete_timestamp"))
        //    existingDriverRecInStateAndDB.withColumn("driverRecordsInDBandStateTeamStream",lit("driverRecordsInDBandStateTeamStream")).show(false)
        }
      //  existingDriverRecInStateAndDB.withColumn("existingDriverRecInStateAndDBTeam",lit("existingDriverRecInStateAndDBTeam")).show(false)

        existingDriverRecInStateAndDB=existingDriverRecInStateAndDB.withColumn("rowNumberCol",row_number.over(Window.partitionBy(col("team_id"),col("driver_id"),col("driver_name")).orderBy(desc("active_flag")))).where("rowNumberCol =1").drop("rowNumberCol")
       // existingDriverRecInStateAndDB.withColumn("driverRecordsInDBTeamStream",lit("driverRecordsInDBTeamStream")).show(false)
        // valid state record check (Expiry)
      /*  Try{teamPersistDFIn.count}.isSuccess*/ teamPersistDFIn match {
          case null =>
            println("No records in state team stream")
          case _ =>
            println("Records in state team stream")
   /*         val stateWithInfoDF=teamPersistDFIn.withColumn("plusMinutes",lit(col("receivedTimeStamp")+expr(inputMap("stateExpiry")))).withColumn("minusMinutes",lit(current_timestamp - expr(inputMap("stateExpiry")))).withColumn("currentTime",lit(current_timestamp))
            stateWithInfoDF.withColumn("stateWithInfoDFTeam",lit("stateWithInfoDFTeam")).show(false)
            val expiredRecords=stateWithInfoDF.filter("plusMinutes <= currentTime")
            val retainedRecords=stateWithInfoDF.where("minusMinutes <= receivedTimeStamp")*/
            val stateWithInfoDF= expiryCheck(teamPersistDFIn,inputMap)
            val expiredRecords=stateWithInfoDF._2
            val retainedRecords=stateWithInfoDF._1
        //    expiredRecords.withColumn("expiredRecordsTeam",lit("expiredRecordsTeam")).show(false)
       //     retainedRecords.withColumn("retainedRecordsTeam",lit("retainedRecordsTeam")).show(false)
       //     expiredRecords.collect.map(x => s"Expired in team state ${x} after expiry check")
       //     retainedRecords.collect.map(x => s"Retained in team state ${x} after expiry check")
            teamPersistDFIn=retainedRecords.drop("plusMinutes","minusMinutes","currentTime")
      //      teamPersistDFIn.withColumn("teamPersistDFIn",lit("teamPersistDFIn")).show(false)
        }

       // existingDriverRecInStateAndDB.printSchema

        // batch team DF vs driver state and DB
        val batchAndDBAndStateDF=batchDF.as("batch").join(existingDriverRecInStateAndDB.as("in_db_state"),$"batch.teamID" === $"in_db_state.team_id" /*Seq("team_id")*/,"left")
        var validDF=batchAndDBAndStateDF.filter("in_db_state.driver_id is not null").selectExpr("batch.*").select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag"))
  //      println(s"Team Stream Schema batchValidDF ${validDF.printSchema}")
        val batchInvalidDF=batchAndDBAndStateDF.filter("in_db_state.driver_id is null").selectExpr("batch.*")
   //     batchAndDBAndStateDF.withColumn("batchAndDBAndStateDFTeam",lit("batchAndDBAndStateDFTeam")).show(false)


        /*Try{teamPersistDFIn.count} */ teamPersistDFIn match {
          case null /*if value.isSuccess == false */=>
            println("Adding records to Team state")
            teamPersistDFIn = batchAndDBAndStateDF.filter("in_db_state.driver_id is null").selectExpr ("batch.*").select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID))
      //      teamPersistDFIn.withColumn("teamPersistDFInCaseFalse",lit("teamPersistDFInCaseFalse")).show(false)
          //   teamPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
          case _ =>
            println("Appending records to Team state")

            //team state release for driver state and db records
            val stateAndDBAndStateDF=teamPersistDFIn.as("state").join(existingDriverRecInStateAndDB.as("in_db_state"),$"state.team_id" === $"in_db_state.team_id" /*Seq("team_id")*/,"left")
            val releasedTeamDF=stateAndDBAndStateDF.filter("in_db_state.driver_id is not null").selectExpr ("state.*")
            val retainedTeamDF=stateAndDBAndStateDF.filter("in_db_state.driver_id is null").selectExpr ("state.*")
      //      retainedTeamDF.collect.map(x => s"Released from teamState ${x} in batch ${batchID}")
      //      releasedTeamDF.collect.map(x => s"Retained in teamState ${x} in batch ${batchID}")
      //      println(s"Team Stream SchemaRetained ${validDF.printSchema}")
      //      println(s"Team Stream Schema batchValidDF Before Union ${validDF.printSchema}")
            validDF=validDF.union(releasedTeamDF.drop("receivedTimeStamp","receivedBatch"))
        //    println(s"Team Stream Schema Retained After Union ${validDF.printSchema}")
            teamPersistDFIn=retainedTeamDF
      //      println(s"Appending records to Team state - before union batch ${batchID}")
            teamPersistDFIn =teamPersistDFIn.union(batchInvalidDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID)))
        //    println(s"Appending records to Team state - after union batch ${batchID}")
       //     teamPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")

   //         teamPersistDFIn.withColumn("teamPersistDFInCaseTrue",lit("teamPersistDFInCaseTrue")).show(false)

            // driver state release for released team records [use outer stream DF to release to get accurate results]
            val driverStateVsReleasedTeam=validDF.as("validTeam").join(driverPersistDFOut.as("driverState"),$"validTeam.team_id"===$"driverState.team_id","right")
            val releasedDriverInTeamStreamDF=driverStateVsReleasedTeam.filter("validTeam.team_id is not null").selectExpr ("driverState.*")
            val retainedDriverInTeamStreamDF=driverStateVsReleasedTeam.filter("validTeam.team_id is null").selectExpr ("driverState.*")

            driverPersistDFInTeam=retainedDriverInTeamStreamDF
            driverPersistDFOut=driverPersistDFInTeam
         //   writeToTable(validDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")),inputMap,inputMap("teamTableName"))

      //      releasedDriverInTeamStreamDF.withColumn("releasedDriverInsideTeamStream",lit("releasedDriverInsideTeamStream")).show(false)
          // for driver
          // invalidate existing record
              //insert new record

            // released data from driver state stream which is resolved by valid team DF
            println("Releasing  data from driver state stream which is resolved by valid team DF")
            updateDriverTable(inputMap,releasedDriverInTeamStreamDF)
          writeToTable(releasedDriverInTeamStreamDF.select(col("team_id"),col("driver_id"),col("driver_name"),col("active_flag")),inputMap,inputMap("driverTableName"))
        }
        teamPersistDFOut=teamPersistDFIn
      //  validDF.withColumn("totalValidTeam",lit("totalValidTeam")).show(false)

        // released valid data from team state and batch
        println("Releasing valid data from team state and batch")
        updateTeamTable(inputMap,validDF)
        writeToTable(validDF.select(col("team_id"),col("team_name"),col("team_valid_flag")),inputMap,inputMap("teamTableName"))
        // for team
        // invalidate existing record
        //insert new record
      //  writeToTable(validDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")),inputMap,inputMap("teamTableName"))
      }
  ).trigger(Trigger.ProcessingTime(10)).start


  driverStreamDF.writeStream.format("console").outputMode("update").option("checkpointLocation",inputMap("driverStreamCheckpoint")).foreachBatch(
    (batchDF:DataFrame,batchID:Long)=>
    {
      var driverPersistDFIn=driverPersistDFOut
      var teamStateInsideDriverStream=teamPersistDFOut
    //  batchDF.withColumn("batchDFDriver",lit("batchDFDriver")).show(false)

      val teamIDIncoming=idGetterTeam(batchDF) //idGetter(batchDF,1)  teamTableName
      inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("teamTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}))a")
     // val existingRecordsInTeamTable=getExistingRecords(inputMap,spark)
      val existingRecordsInTeamTable=getExistingRecords(inputMap,spark,s"(select * from ${inputMap("databaseName")}.${inputMap("teamTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}))a")
    //  existingRecordsInTeamTable.withColumn("existingRecordsInTeamTableDriver",lit("existingRecordsInTeamTableDriver")).show(false)
      var existingTeamRecInStateAndDB:DataFrame=null
     // Try{teamPersistDFOut.count}.isSuccess match {
      teamPersistDFOut match  {
        case null =>
          existingTeamRecInStateAndDB=existingRecordsInTeamTable.drop("delete_timestamp","insert_timestamp").select(col("team_id"),col("team_name"),col("team_valid_flag"))
       //   existingTeamRecInStateAndDB.withColumn("existingTeamRecInStateAnDBDriver",lit("existingTeamRecInStateAnDBDriver")).show(false)
        case _ =>
          existingTeamRecInStateAndDB=(teamPersistDFOut.drop("receivedTimeStamp","receivedBatch").select(col("team_id"),col("team_name"),col("team_valid_flag"))).union(existingRecordsInTeamTable.drop("delete_timestamp","insert_timestamp").select(col("team_id"),col("team_name"),col("team_valid_flag")))
          // existingTeamRecInStateAndDB.withColumn("existingTeamRecInStateAnDBDriver",lit("existingTeamRecInStateAnDBDriver")).show(false)
      }

      // valid state record check
//      existingTeamRecInStateAndDB.withColumn("existingTeamRecInStateAnDBDriver",lit("existingTeamRecInStateAnDBDriver")).show(false)
   // if duplicates are there in state Elimiate it here
     existingTeamRecInStateAndDB=existingTeamRecInStateAndDB.withColumn("rowNumberCol",row_number.over(Window.partitionBy(col("team_id"),col("team_name")).orderBy(desc("team_valid_flag")))).where("rowNumberCol =1").drop("rowNumberCol")
   //   existingTeamRecInStateAndDB.withColumn("existingTeamRecInStateAnDBDriver",lit("existingTeamRecInStateAnDBDriver")).show(false)
      /* Try{driverPersistDFIn.count}.isSuccess */driverPersistDFIn match {
        case null =>
          println("No records in state driver stream")
        case _ =>
          println("Records in state driver stream")
/*          val stateWithInfoDF=driverPersistDFIn.withColumn("plusMinutes",lit(col("receivedTimeStamp")+expr(inputMap("stateExpiry")))).withColumn("minusMinutes",lit(current_timestamp - expr(inputMap("stateExpiry")))).withColumn("currentTime",lit(current_timestamp))
          stateWithInfoDF.withColumn("stateWithInfoDFDriver",lit("stateWithInfoDFDriver")).show(false)
          val expiredRecords=stateWithInfoDF.filter("plusMinutes <= currentTime")
          val retainedRecords=stateWithInfoDF.where("minusMinutes <= receivedTimeStamp")*/
          val stateWithInfoDF= expiryCheck(driverPersistDFIn,inputMap)
          val expiredRecords=stateWithInfoDF._2
          val retainedRecords=stateWithInfoDF._1
        //  expiredRecords.withColumn("expiredRecordsDriver",lit("expiredRecordsDriver")).show(false)
        //  retainedRecords.withColumn("retainedRecordsDriver",lit("retainedRecordsDriver")).show(false)
         // expiredRecords.collect.map(x => s"Expired in driver state ${x} after expiry check")
         // retainedRecords.collect.map(x => s"Retained in driver state ${x} after expiry check")
          driverPersistDFIn=retainedRecords.drop("plusMinutes","minusMinutes","currentTime")
        //  driverPersistDFIn.withColumn("driverPersistDFIn",lit("driverPersistDFIn")).show(false)
      }

      // batch vs state and db
      val batchAndDBAndStateDF=batchDF.as("batch").join(existingTeamRecInStateAndDB.as("in_db_state"),$"batch.teamID" === $"in_db_state.team_id"/* Seq("team_id")*/,"left")
 //     batchAndDBAndStateDF.withColumn("batchAndDBAndStateDFDriver",lit("batchAndDBAndStateDFDriver")).show(false)
      //      existingTeamRecInStateAndDB.printSchema
      var validDF=batchAndDBAndStateDF.filter("in_db_state.team_name is not null").selectExpr("batch.*").select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag"))
      val batchInvalid=batchAndDBAndStateDF.filter("in_db_state.team_name is null").selectExpr("batch.*")
  //    println(s"Driver Stream Schema batchValidDF ${validDF.printSchema}")


      driverPersistDFIn match {
        case /*value if value.isSuccess == false*/ null =>
          println("Adding records to Driver state")
          driverPersistDFIn = batchAndDBAndStateDF.filter("in_db_state.team_name is null").selectExpr ("batch.*").select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID))
        //  driverPersistDFIn.withColumn("driverPersistDFInTryFalse",lit("driverPersistDFInTryFalse")).show(false)
        case _ =>
          println("Appending records to Driver state")
          val stateAndDBAndStateDF=driverPersistDFIn.as("state").join(existingTeamRecInStateAndDB.as("in_db_state"),$"state.team_id" === $"in_db_state.team_id" /*Seq("team_id")*/,"left")
          val releasedDF=stateAndDBAndStateDF.filter("in_db_state.team_name is not null").selectExpr( "state.*")
          val retainedDF=stateAndDBAndStateDF.filter("in_db_state.team_name is null").selectExpr( "state.*")
        //  releasedDF.collect.map(x => s"Released from driverState ${x} in batch ${batchID}")
        //  retainedDF.collect.map(x => s"Retained in driverState ${x} in batch ${batchID}")
       //   println(s"Driver Stream Schema releasedDF ${releasedDF.printSchema}")
       //   println(s"Driver Stream Schema batchValidDF Before Union ${validDF.printSchema}")
          validDF=validDF.union(releasedDF.drop("receivedTimeStamp","receivedBatch"))
  //        println(s"Driver Stream Schema batchValidDF After Union ${validDF.printSchema}")
          driverPersistDFIn=retainedDF
       //   println(s"Appending records to Driver state - before union batch ${batchID}")
          driverPersistDFIn =driverPersistDFIn.union(batchInvalid.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID)))
       //   println(s"Appending records to Driver state - after union batch ${batchID}")
      //    driverPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
    //      driverPersistDFIn.withColumn("driverPersistDFInTryTrue",lit("driverPersistDFInTryTrue")).show(false)

          // checking released driver vs team state [take outer state for this]
          val validTeamDFCheck=validDF.as("validBatchDriver").join(teamPersistDFOut.as("teamState"),$"validBatchDriver.team_id"===$"teamState.team_id","right")
          val teamReleaseRecords=validTeamDFCheck.filter("validBatchDriver.driver_id is not null").selectExpr("teamState.*")
          val teamRetailRecords=validTeamDFCheck.filter("validBatchDriver.driver_id is null").selectExpr("teamState.*")

          teamStateInsideDriverStream=teamRetailRecords
          teamPersistDFOut=teamStateInsideDriverStream

          // released data from team state which is resolved by valid driver DF
          println("Releasing data from team state which is resolved by valid driver DF")
          updateTeamTable(inputMap,teamReleaseRecords)
          writeToTable(teamReleaseRecords.select(col("team_id"),col("team_name"),col("team_valid_flag")),inputMap,inputMap("teamTableName"))
        //  teamReleaseRecords.withColumn("teamReleasedInsideDriverState",lit("teamReleasedInsideDriverState")).show(false)
      }

      driverPersistDFOut=driverPersistDFIn

      // released data from driver batch and state
      println("Releasing valid data from driver batch and state")

      updateDriverTable(inputMap,validDF)
      writeToTable(validDF.select(col("team_id"),col("driver_id"),col("driver_name"),col("active_flag")),inputMap,inputMap("driverTableName"))

     //  validDF.withColumn("totalValidDriver",lit("totalValidDriver")).show(false)
  //    writeToTable(validDF.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")),inputMap,inputMap("driverTableName"))
    }
  ).trigger(Trigger.ProcessingTime(10)).start

spark.streams.awaitAnyTermination

  /*

  Resolves both sides
  team id must be present in driver table or state for team record
  driver's team id must be present in driver table or state for driver record

  2 releases:
  released the driver records from state in team stream if team records get resolved
  released the driver records from state in driver stream if team records are in team state till prev batch or in db

  2 releases:
  released the team records from state in driver stream if driver records get resolved
  released the team records from state in team stream if driver records are in driver state till prev batch or in db

  2 updates and inserts for each type of records.
  Driver record is updated and inserted in team stream if team batch or team state gets resolved with driver state
  Driver record is updated and inserted in driver stream if resolved with team state or team db.

  2 updates and inserts for each type of records.
  team record is updated and inserted in driver stream if driver batch or driver state gets resolved with team state
  team record is updated and inserted in team stream if resolved with driver state or driver db.


Batch 0:
{"eventInfo":"driverEvent","eventData":"{\"driverId\":\"D005\",\"teamId\":\"T005\",\"driverName\":\"Senna\",\"activeFlag\":\"Y\"}"}
Batch 1:
{"eventInfo":"teamEvent","eventData":"{\"teamName\":\"Alpine Racing\",\"teamId\":\"T005\",\"checkFlag\":\"Y\"}"}
Batch 2:
{"eventInfo":"driverEvent","eventData":"{\"driverId\":\"D007\",\"teamId\":\"T005\",\"driverName\":\"Senna-2\",\"activeFlag\":\"Y\"}"}
  */

}
}
