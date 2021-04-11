package org.controller.persistingInsideJob

import org.util.SparkOpener
import jobHelper._
import jobConstantsPersist._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
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
        batchDF.withColumn("batchDFTeam",lit("batchDFTeam")).show(false)
        val teamIDIncoming=idGetterTeam(batchDF) //idGetter(batchDF,1)  teamTableName
        inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("driverTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}) )a")
      //  val existingRecordsInDriverTable=getExistingRecords(inputMap,spark)
        val existingRecordsInDriverTable=getExistingRecords(inputMap,spark,s"(select * from ${inputMap("databaseName")}.${inputMap("driverTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}) )a")
        existingRecordsInDriverTable.withColumn("existingRecordsInDriverTableTeam",lit("existingRecordsInDriverTeam")).show(false)
        var existingDriverRecInStateAndDB:DataFrame=null

        // appending existing state driver with db driver records
        Try{driverPersistDFInTeam.count}.isSuccess match {
          case false =>
            println("No records in driver state team stream")
            existingDriverRecInStateAndDB=existingRecordsInDriverTable.drop("insert_timestamp","delete_timestamp")
            existingDriverRecInStateAndDB.withColumn("driverRecordsInDBTeamStream",lit("driverRecordsInDBTeamStream")).show(false)
          case true =>
            println("Records in driver state team stream")
            existingDriverRecInStateAndDB=(driverPersistDFInTeam.drop("receivedTimeStamp","receivedBatch")).union(existingRecordsInDriverTable.drop("insert_timestamp","delete_timestamp"))
            existingDriverRecInStateAndDB.withColumn("driverRecordsInDBandStateTeamStream",lit("driverRecordsInDBandStateTeamStream")).show(false)
        }
        existingDriverRecInStateAndDB.withColumn("existingDriverRecInStateAndDBTeam",lit("existingDriverRecInStateAndDBTeam")).show(false)

        // valid state record check (Expiry)
        Try{teamPersistDFIn.count}.isSuccess match {
          case false =>
            println("No records in state team stream")
          case true =>
            println("Records in state team stream")
   /*         val stateWithInfoDF=teamPersistDFIn.withColumn("plusMinutes",lit(col("receivedTimeStamp")+expr(inputMap("stateExpiry")))).withColumn("minusMinutes",lit(current_timestamp - expr(inputMap("stateExpiry")))).withColumn("currentTime",lit(current_timestamp))
            stateWithInfoDF.withColumn("stateWithInfoDFTeam",lit("stateWithInfoDFTeam")).show(false)
            val expiredRecords=stateWithInfoDF.filter("plusMinutes <= currentTime")
            val retainedRecords=stateWithInfoDF.where("minusMinutes <= receivedTimeStamp")*/
            val stateWithInfoDF= expiryCheck(teamPersistDFIn,inputMap)
            val expiredRecords=stateWithInfoDF._2
            val retainedRecords=stateWithInfoDF._1
            expiredRecords.withColumn("expiredRecordsTeam",lit("expiredRecordsTeam")).show(false)
            retainedRecords.withColumn("retainedRecordsTeam",lit("retainedRecordsTeam")).show(false)
       //     expiredRecords.collect.map(x => s"Expired in team state ${x} after expiry check")
       //     retainedRecords.collect.map(x => s"Retained in team state ${x} after expiry check")
            teamPersistDFIn=retainedRecords.drop("plusMinutes","minusMinutes","currentTime")
            teamPersistDFIn.withColumn("teamPersistDFIn",lit("teamPersistDFIn")).show(false)
        }

       // existingDriverRecInStateAndDB.printSchema

        // batch team DF vs driver state and DB
        val batchAndDBAndStateDF=batchDF.as("batch").join(existingDriverRecInStateAndDB.as("in_db_state"),$"batch.teamID" === $"in_db_state.team_id" /*Seq("team_id")*/,"left")
        var validDF=batchAndDBAndStateDF.filter("in_db_state.driver_id is not null").selectExpr("batch.*")
        println(s"Team Stream Schema batchValidDF ${validDF.printSchema}")
        val batchInvalidDF=batchAndDBAndStateDF.filter("in_db_state.driver_id is null").selectExpr("batch.*")
        batchAndDBAndStateDF.withColumn("batchAndDBAndStateDFTeam",lit("batchAndDBAndStateDFTeam")).show(false)


        Try{teamPersistDFIn.count} match {
          case value if value.isSuccess == false =>
            println("Adding records to Team state")
            teamPersistDFIn = batchAndDBAndStateDF.filter("in_db_state.driver_id is null").selectExpr ("batch.*").select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID))
            teamPersistDFIn.withColumn("teamPersistDFInCaseFalse",lit("teamPersistDFInCaseFalse")).show(false)
          //   teamPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
          case _ =>
            println("Appending records to Team state")

            //team state release for driver state and db records
            val stateAndDBAndStateDF=teamPersistDFIn.as("state").join(existingDriverRecInStateAndDB.as("in_db_state"),$"state.team_id" === $"in_db_state.team_id" /*Seq("team_id")*/,"left")
            val releasedTeamDF=stateAndDBAndStateDF.filter("in_db_state.driver_id is not null").selectExpr ("state.*")
            val retainedTeamDF=stateAndDBAndStateDF.filter("in_db_state.driver_id is null").selectExpr ("state.*")
            retainedTeamDF.collect.map(x => s"Released from teamState ${x} in batch ${batchID}")
            releasedTeamDF.collect.map(x => s"Retained in teamState ${x} in batch ${batchID}")
            println(s"Team Stream SchemaRetained ${validDF.printSchema}")
            println(s"Team Stream Schema batchValidDF Before Union ${validDF.printSchema}")
            validDF=validDF.union(releasedTeamDF.drop("receivedTimeStamp","receivedBatch"))
            println(s"Team Stream Schema Retained After Union ${validDF.printSchema}")
            teamPersistDFIn=retainedTeamDF
            println(s"Appending records to Team state - before union batch ${batchID}")
            teamPersistDFIn =teamPersistDFIn.union(batchInvalidDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID)))
            println(s"Appending records to Team state - after union batch ${batchID}")
            teamPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")

            teamPersistDFIn.withColumn("teamPersistDFInCaseTrue",lit("teamPersistDFInCaseTrue")).show(false)

            // driver state release for released team records
            val driverStateVsReleasedTeam=releasedTeamDF.as("releasedTeam").join(driverPersistDFInTeam.as("driverState"),$"releasedTeam.team_id"===$"driverState.team_id","right")
            val releasedDriverInTeamStreamDF=driverStateVsReleasedTeam.filter("releasedTeam.team_id is not null").selectExpr ("driverState.*")
            val retainedDriverInTeamStreamDF=driverStateVsReleasedTeam.filter("releasedTeam.team_id is null").selectExpr ("driverState.*")
            driverPersistDFInTeam=retainedDriverInTeamStreamDF

            driverPersistDFOut=driverPersistDFInTeam
         //   writeToTable(validDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")),inputMap,inputMap("teamTableName"))

            releasedDriverInTeamStreamDF.withColumn("releasedDriverInsideTeamStream",lit("releasedDriverInsideTeamStream")).show(false)
          // for driver
          // invalidate existing record
              //insert new record
        }
        teamPersistDFOut=teamPersistDFIn
        validDF.withColumn("totalValidTeam",lit("totalValidTeam")).show(false)
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
      batchDF.withColumn("batchDFDriver",lit("batchDFDriver")).show(false)

      val teamIDIncoming=idGetterTeam(batchDF) //idGetter(batchDF,1)  teamTableName
      inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("teamTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}))a")
     // val existingRecordsInTeamTable=getExistingRecords(inputMap,spark)
      val existingRecordsInTeamTable=getExistingRecords(inputMap,spark,s"(select * from ${inputMap("databaseName")}.${inputMap("teamTableName")} where team_id in (${idListToStringManipulator(teamIDIncoming)}))a")
      existingRecordsInTeamTable.withColumn("existingRecordsInTeamTableDriver",lit("existingRecordsInTeamTableDriver")).show(false)
      var existingTeamRecInStateAndDB:DataFrame=null
      Try{teamPersistDFOut.count}.isSuccess match {
        case false => existingTeamRecInStateAndDB=existingRecordsInTeamTable.drop("delete_timestamp","insert_timestamp")
        case true => existingTeamRecInStateAndDB=(teamPersistDFOut.drop("receivedTimeStamp","receivedBatch")).union(existingRecordsInTeamTable.drop("delete_timestamp","insert_timestamp"))
      }

      // valid state record check
      existingTeamRecInStateAndDB.withColumn("existingTeamRecInStateAnDBDriver",lit("existingTeamRecInStateAnDBDriver")).show(false)

      Try{driverPersistDFIn.count}.isSuccess match {
        case false =>
          println("No records in state driver stream")
        case true =>
          println("Records in state driver stream")
/*          val stateWithInfoDF=driverPersistDFIn.withColumn("plusMinutes",lit(col("receivedTimeStamp")+expr(inputMap("stateExpiry")))).withColumn("minusMinutes",lit(current_timestamp - expr(inputMap("stateExpiry")))).withColumn("currentTime",lit(current_timestamp))
          stateWithInfoDF.withColumn("stateWithInfoDFDriver",lit("stateWithInfoDFDriver")).show(false)
          val expiredRecords=stateWithInfoDF.filter("plusMinutes <= currentTime")
          val retainedRecords=stateWithInfoDF.where("minusMinutes <= receivedTimeStamp")*/
          val stateWithInfoDF= expiryCheck(driverPersistDFIn,inputMap)
          val expiredRecords=stateWithInfoDF._2
          val retainedRecords=stateWithInfoDF._1
          expiredRecords.withColumn("expiredRecordsDriver",lit("expiredRecordsDriver")).show(false)
          retainedRecords.withColumn("retainedRecordsDriver",lit("retainedRecordsDriver")).show(false)
         // expiredRecords.collect.map(x => s"Expired in driver state ${x} after expiry check")
         // retainedRecords.collect.map(x => s"Retained in driver state ${x} after expiry check")
          driverPersistDFIn=retainedRecords.drop("plusMinutes","minusMinutes","currentTime")
          driverPersistDFIn.withColumn("driverPersistDFIn",lit("driverPersistDFIn")).show(false)
      }

      // batch vs state and db
      val batchAndDBAndStateDF=batchDF.as("batch").join(existingTeamRecInStateAndDB.as("in_db_state"),$"batch.teamID" === $"in_db_state.team_id"/* Seq("team_id")*/,"left")
      batchAndDBAndStateDF.withColumn("batchAndDBAndStateDFDriver",lit("batchAndDBAndStateDFDriver")).show(false)
      //      existingTeamRecInStateAndDB.printSchema
      var validDF=batchAndDBAndStateDF.filter("in_db_state.team_name is not null").selectExpr("batch.*")
      val batchInvalid=batchAndDBAndStateDF.filter("in_db_state.team_name is null").selectExpr("batch.*")
      println(s"Driver Stream Schema batchValidDF ${validDF.printSchema}")


      Try{driverPersistDFIn.count} match {
        case value if value.isSuccess == false =>
          println("Adding records to Driver state")
          driverPersistDFIn = batchAndDBAndStateDF.filter("in_db_state.team_name is null").selectExpr ("batch.*").select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID))
          driverPersistDFIn.withColumn("driverPersistDFInTryFalse",lit("driverPersistDFInTryFalse")).show(false)
        case _ =>
          println("Appending records to Driver state")
          val stateAndDBAndStateDF=driverPersistDFIn.as("state").join(existingTeamRecInStateAndDB.as("in_db_state"),$"state.team_id" === $"in_db_state.team_id" /*Seq("team_id")*/,"left")
          val releasedDF=stateAndDBAndStateDF.filter("in_db_state.team_name is not null").selectExpr( "state.*")
          val retainedDF=stateAndDBAndStateDF.filter("in_db_state.team_name is null").selectExpr( "state.*")
          releasedDF.collect.map(x => s"Released from driverState ${x} in batch ${batchID}")
          retainedDF.collect.map(x => s"Retained in driverState ${x} in batch ${batchID}")
          println(s"Driver Stream Schema releasedDF ${releasedDF.printSchema}")
          println(s"Driver Stream Schema batchValidDF Before Union ${validDF.printSchema}")
          validDF=validDF.union(releasedDF.drop("receivedTimeStamp","receivedBatch"))
          println(s"Driver Stream Schema batchValidDF After Union ${validDF.printSchema}")
          driverPersistDFIn=retainedDF
          println(s"Appending records to Driver state - before union batch ${batchID}")
          driverPersistDFIn =driverPersistDFIn.union(batchInvalid.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")).withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatch",lit(batchID)))
          println(s"Appending records to Driver state - after union batch ${batchID}")
          driverPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
          driverPersistDFIn.withColumn("driverPersistDFInTryTrue",lit("driverPersistDFInTryTrue")).show(false)

          // checking released driver vs team state
          val validTeamDFCheck=validDF.as("validBatchDriver").join(teamStateInsideDriverStream.as("teamState"),$"validBatchDriver.teamId"===$"teamState.team_id","right")
          val teamReleaseRecords=validTeamDFCheck.filter("validBatchDriver.driverId is not null").selectExpr("teamState.*")
          val teamRetailRecords=validTeamDFCheck.filter("validBatchDriver.driverId is null").selectExpr("teamState.*")
          teamStateInsideDriverStream=teamRetailRecords

          teamPersistDFOut=teamStateInsideDriverStream

          teamReleaseRecords.withColumn("teamReleasedInsideDriverState",lit("teamReleasedInsideDriverState")).show(false)
      }

      driverPersistDFOut=driverPersistDFIn

      validDF.withColumn("totalValidDriver",lit("totalValidDriver")).show(false)
  //    writeToTable(validDF.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")),inputMap,inputMap("driverTableName"))
    }
  ).trigger(Trigger.ProcessingTime(10)).start

spark.streams.awaitAnyTermination

  /*
  Doesn't work properly as dag is running in parallel  for 2 streams, need to implement[ only state ]kind of logic
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.23 --class org.controller.persistingInsideJob.combinedResolving --num-executors 2 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 2 --deploy-mode client --master local[*] --conf 'spark.driver.extraJavaOptions=-DXmx=512m' --conf 'spark.driver.extraJavaOptions=-DXms=64m' /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServer=localhost:9092,localhost:9093,localhost:9094 topics=driverInfoTopic,teamInfoTopic driverStreamCheckpoint=hdfs://localhost:8020/user/raptor/streams/tstDriver/ teamStreamCheckpoint=hdfs://localhost:8020/user/raptor/streams/tstTeam/ offsetForTopic=latest driverMYSQL="com.mysql.cj.jdbc.Driver" username=raptor password= urlJDBC="jdbc:mysql://localhost:3306/testPersist" databaseName=testPersist teamTableName=team_info driverTableName=driver_info stateExpiry="INTERVAL 15 MINUTE"

  */

}
}
