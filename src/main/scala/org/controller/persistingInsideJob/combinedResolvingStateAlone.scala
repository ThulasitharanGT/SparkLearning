package org.controller.persistingInsideJob

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, current_timestamp, desc, from_json, length, lit, row_number}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.controller.persistingInsideJob.combinedResolving.SparkSessionLoc
import org.controller.persistingInsideJob.jobConstantsPersist.{driverEvent, driverSchema, schemaOfMessage, teamEvent, teamSchema}
import org.controller.persistingInsideJob.jobHelper.{argsToMapConvert, expiryCheck, getExistingRecords, idGetterTeam, idListToStringManipulator, updateDriverTable, updateTeamTable, writeToTable}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Try

object combinedResolvingStateAlone {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionLoc()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val inputMap = argsToMapConvert(args)
    val readStreamDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", inputMap("bootstrapServer")).option("subscribe", inputMap("topics")).option("offset", inputMap("offsetForTopic")).load.select(from_json(col("value").cast(StringType), schemaOfMessage).as("convertedMessage")).selectExpr("convertedMessage.*")

    val teamStreamDF = readStreamDF.where(s"eventInfo='${teamEvent}'").select(from_json(col("eventData").cast(StringType), teamSchema).as("teamData")).selectExpr("teamData.*")
    val driverStreamDF = readStreamDF.where(s"eventInfo='${driverEvent}'").select(from_json(col("eventData").cast(StringType), driverSchema).as("driverData")).selectExpr("driverData.*")

    var driverPersistDFOut: DataFrame = null
    var teamPersistDFOut: DataFrame = null

    teamStreamDF.writeStream.format("console").outputMode("update").option("checkpointLocation", inputMap("teamStreamCheckpoint")).foreachBatch(
      (batchDF: DataFrame, batchID: Long) => {
        var teamPersistDFIn = teamPersistDFOut
        batchDF.withColumn("batchDFTeam", lit("batchDFTeam")).show(false)

        var existingDriverRecInState: DataFrame = null

        // appending existing state driver with db driver records
      /*  Try {
          driverPersistDFOut.count
        }.isSuccess*/ driverPersistDFOut match {
          case null =>
            println("No records in driver state team stream")
             // dummyDF
            val dateObj=new Date
          //  val formatObj= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
           // formatObj.format(dateObj)
            val tmpTimeStamp=new Timestamp(dateObj.getTime())
            existingDriverRecInState = Seq(("","","","",tmpTimeStamp,0)).toDF("team_id","driver_id","driver_name","active_flag","receivedTimeStamp", "receivedBatch").filter(length(col("driver_name")) >0).drop("receivedTimeStamp", "receivedBatch")
          // Seq(("","","","",tmpTimeStamp,0)).toDF("team_id","driver_id","driver_name","active_flag","receivedTimeStamp", "receivedBatch").filter("length(driver_name) >0")

          case _ =>
            println("Records in driver state team stream")
            existingDriverRecInState = driverPersistDFOut.drop("receivedTimeStamp", "receivedBatch").withColumn("rowNumberCol",row_number.over(Window.partitionBy(col("team_id"),col("driver_id"),col("driver_name")).orderBy(desc("active_flag")))).where("rowNumberCol =1").drop("rowNumberCol")
         //   existingDriverRecInState.withColumn("driverRecordsInDBandStateTeamStream", lit("driverRecordsInDBandStateTeamStream")).show(false)
        }
        // valid state record check (Expiry)
        /*Try {
          teamPersistDFIn.count
        }.isSuccess */ teamPersistDFIn match {
          case null =>
            println("No records in state team stream")
          case _ =>
            println("Records in state team stream")
            val stateWithInfoDF = expiryCheck(teamPersistDFIn, inputMap)
            val expiredRecords = stateWithInfoDF._2
            val retainedRecords = stateWithInfoDF._1
      //      expiredRecords.withColumn("expiredRecordsTeam", lit("expiredRecordsTeam")).show(false)
       //     retainedRecords.withColumn("retainedRecordsTeam", lit("retainedRecordsTeam")).show(false)
                expiredRecords.collect.map(x => s"Expired in team state ${x} after expiry check")
                retainedRecords.collect.map(x => s"Retained in team state ${x} after expiry check")
            teamPersistDFIn = retainedRecords.drop("plusMinutes", "minusMinutes", "currentTime")
          //  teamPersistDFIn.withColumn("teamPersistDFInRetained", lit("teamPersistDFInRetained")).show(false)
        }

        // batch team DF vs driver state
        val batchAndStateDF = batchDF.as("batch").join(existingDriverRecInState.as("in_state"), $"batch.teamID" === $"in_state.team_id" /*Seq("team_id")*/ , "left")
        var validDF = batchAndStateDF.filter("in_state.driver_id is not null").selectExpr("batch.*")
        println(s"Team Stream Schema batchValidDF ${validDF.printSchema}")
        val batchInvalidDF = batchAndStateDF.filter("in_state.driver_id is null").selectExpr("batch.*")
     //   batchAndStateDF.withColumn("batchAndDBAndStateDFTeam", lit("batchAndDBAndStateDFTeam")).show(false)


      /*  Try {
          teamPersistDFIn.count
        }*/ teamPersistDFIn match {
          case /*value if value.isSuccess == false*/ null =>
            println("Adding records to Team state")
            teamPersistDFIn = batchInvalidDF.selectExpr("batch.*").select(col("teamId").as("team_id"), col("teamName").as("team_name"), col("checkFlag").as("team_valid_flag")).withColumn("receivedTimeStamp", lit(current_timestamp)).withColumn("receivedBatch", lit(batchID))
          //  teamPersistDFIn.withColumn("teamPersistDFInCaseFalse", lit("teamPersistDFInCaseFalse")).show(false)
          //   teamPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
          case _ =>
            println("Appending records to Team state")

            //team state release for driver state and db records
            val stateAndStateDF = teamPersistDFIn.as("state").join(existingDriverRecInState.as("in_state"), $"state.team_id" === $"in_state.team_id" /*Seq("team_id")*/ , "left")
            val releasedTeamDF = stateAndStateDF.filter("in_state.driver_id is not null").selectExpr("state.*")
            val retainedTeamDF = stateAndStateDF.filter("in_state.driver_id is null").selectExpr("state.*")
            retainedTeamDF.collect.map(x => s"Released from teamState ${x} in batch ${batchID}")
            releasedTeamDF.collect.map(x => s"Retained in teamState ${x} in batch ${batchID}")
            //println(s"Team Stream SchemaRetained ${validDF.printSchema}")
            //println(s"Team Stream Schema batchValidDF Before Union ${validDF.printSchema}")
            validDF = validDF.union(releasedTeamDF.drop("receivedTimeStamp", "receivedBatch"))
             println(s"Team Stream Schema Retained After Union ${validDF.printSchema}")
            teamPersistDFIn = retainedTeamDF
            // println(s"Appending records to Team state - before union batch ${batchID}")
            println(s"Team Stream batchInvalidDF printSchema ${batchInvalidDF.printSchema}")
            teamPersistDFIn = teamPersistDFIn.union(batchInvalidDF.withColumn("receivedTimeStamp", lit(current_timestamp)).withColumn("receivedBatch", lit(batchID)))
            // println(s"Appending records to Team state - after union batch ${batchID}")
            teamPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
       //     teamPersistDFIn.withColumn("teamPersistDFInCaseTrue", lit("teamPersistDFInCaseTrue")).show(false)

    /*        // driver release
            val releasedDriverState = stateAndStateDF.where("in_state.team_id is not null").selectExpr("in_state.*") //.drop("receivedTimeStamp","receivedBatch")
            val releaseDriverBatch=batchAndStateDF.where("in_state.team_id is not null").selectExpr("in_state.*")

            //  val retainedDriver = existingDriverRecInState.except(stateAndStateDF)

            val retainedDriverState = driverPersistDFOut.as("state_out").join(releasedDriverState.as("released_in"),$"state_out.team_id"===$"released_in.team_id","left").where("released_in.team_id is null").selectExpr("state_out.*")
            val retainedDriverBatch = driverPersistDFOut.as("state_out").join(releaseDriverBatch.as("released_in"),$"state_out.team_id"===$"released_in.teamId","left").where("released_in.teamId is null").selectExpr("state_out.*")

            val driverRetainedStateAndBatch= releasedDriverState.union(releaseDriverBatch)

    */
            val driverStateAndValid=driverPersistDFOut.as("state_out").join(validDF.as("released_in"),$"state_out.team_id"===$"released_in.teamId","left")
            val retainedDriver = driverStateAndValid.where("released_in.teamId is null").selectExpr("state_out.*")
            val releasedDriver = driverStateAndValid.where("released_in.teamId is not null").selectExpr("state_out.*")//.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag"))

            driverStateAndValid.withColumn("driverStateAndValid",lit("driverStateAndValid")).show(false)

            retainedDriver.persist(StorageLevel.MEMORY_AND_DISK_SER)
            driverPersistDFOut = retainedDriver
           releasedDriver.drop("receivedTimeStamp", "receivedBatch").withColumn("releasedDriverInTeamState", lit("releasedDriverInTeamState")).show(false)
           println(s"printSchema of released driver in team ${releasedDriver.printSchema}")
          // write released records into driver table, update delts and insert
            updateDriverTable(inputMap,releasedDriver)
            writeToTable(releasedDriver.drop("receivedBatch","receivedTimeStamp")/*.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag"))*/,inputMap,inputMap("driverTableName"))
          //   writeToTable(validDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")),inputMap,inputMap("teamTableName"))

        }
        teamPersistDFOut = teamPersistDFIn
        updateTeamTable(inputMap,validDF)
        writeToTable(validDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")),inputMap,inputMap("teamTableName"))
        println(s"printSchema of valid team ${validDF.printSchema}")

        validDF.withColumn("totalValidTeam", lit("totalValidTeam")).show(false)
        // for team
        // invalidate existing record
        //insert new record
        //  writeToTable(validDF.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag")),inputMap,inputMap("teamTableName"))
      }
    ).trigger(Trigger.ProcessingTime(10)).start


    driverStreamDF.writeStream.format("console").outputMode("update").option("checkpointLocation", inputMap("driverStreamCheckpoint")).foreachBatch(
      (batchDF: DataFrame, batchID: Long) => {
        var driverPersistDFIn = driverPersistDFOut

        batchDF.withColumn("batchDFDriver", lit("batchDFDriver")).show(false)

        var existingTeamRecInState: DataFrame = null
       /* Try {
          teamPersistDFOut.count


        }.isSuccess*/ teamPersistDFOut match {
          case null =>
            println("No records in team state")
            val dateObj=new Date
            //  val formatObj= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
            // formatObj.format(dateObj)
            val tmpTimeStamp=new Timestamp(dateObj.getTime())
            existingTeamRecInState=Seq(("","","",tmpTimeStamp,0)).toDF("team_id","team_name","team_valid_flag","receivedTimeStamp", "receivedBatch").filter(length(col("team_valid_flag")) >0).drop("receivedTimeStamp", "receivedBatch")
          case _ =>
            existingTeamRecInState = teamPersistDFOut.drop("receivedTimeStamp", "receivedBatch").withColumn("rowNumberCol",row_number.over(Window.partitionBy(col("team_id"),col("team_name")).orderBy(desc("team_valid_flag")))).where("rowNumberCol =1").drop("rowNumberCol")
        }

        // valid state record check
       // existingTeamRecInState.withColumn("existingTeamRecInState", lit("existingTeamRecInState")).show(false)

      /*  Try {
          driverPersistDFIn.count
        }.isSuccess */ driverPersistDFIn match {
          case null =>
            println("No records in state driver stream")
          case _ =>
            println("Records in state driver stream")

            val stateWithInfoDF = expiryCheck(driverPersistDFIn, inputMap)
            val expiredRecords = stateWithInfoDF._2
            val retainedRecords = stateWithInfoDF._1
          //  expiredRecords.withColumn("expiredRecordsDriver", lit("expiredRecordsDriver")).show(false)
         //   retainedRecords.withColumn("retainedRecordsDriver", lit("retainedRecordsDriver")).show(false)
             expiredRecords.collect.map(x => s"Expired in driver state ${x} after expiry check")
             retainedRecords.collect.map(x => s"Retained in driver state ${x} after expiry check")
            driverPersistDFIn = retainedRecords.drop("plusMinutes", "minusMinutes", "currentTime")
         //   driverPersistDFIn.withColumn("driverPersistDFIn", lit("driverPersistDFIn")).show(false)
        }

        // batch vs state
        val batchAndStateDF = batchDF.as("batch").join(existingTeamRecInState.as("in_state"), $"batch.teamID" === $"in_state.team_id" /* Seq("team_id")*/ , "left")
     ///   batchAndStateDF.withColumn("batchAndStateDF", lit("batchAndStateDF")).show(false)
        //      existingTeamRecInStateAndDB.printSchema
        var validDF = batchAndStateDF.filter("in_state.team_name is not null").selectExpr("batch.*")
        val batchInvalid = batchAndStateDF.filter("in_state.team_name is null").selectExpr("batch.*")
        // println(s"Driver Stream Schema batchValidDF ${validDF.printSchema}")

 /*       Try {
          driverPersistDFIn.count
        } */driverPersistDFIn match {
          case /*value if value.isSuccess == false */ null=>
            println("Adding records to Driver state")
            driverPersistDFIn = batchInvalid.selectExpr("batch.*").select(col("teamId").as("team_id"), col("driverId").as("driver_id"), col("driverName").as("driver_name"), col("activeFlag").as("active_flag")).withColumn("receivedTimeStamp", lit(current_timestamp)).withColumn("receivedBatch", lit(batchID))
          //  driverPersistDFIn.withColumn("driverPersistDFInTryFalse", lit("driverPersistDFInTryFalse")).show(false)
          case _ =>
            println("Appending records to Driver state")
            val stateAndStateDF = driverPersistDFIn.as("state").join(existingTeamRecInState.as("in_state"), $"state.team_id" === $"in_state.team_id" /*Seq("team_id")*/ , "left")
            val releasedDF = stateAndStateDF.filter("in_state.team_name is not null").selectExpr("state.*")
            val retainedDF = stateAndStateDF.filter("in_state.team_name is null").selectExpr("state.*")
            //   releasedDF.collect.map(x => s"Released from driverState ${x} in batch ${batchID}")
            //   retainedDF.collect.map(x => s"Retained in driverState ${x} in batch ${batchID}")
            //   println(s"Driver Stream Schema releasedDF ${releasedDF.printSchema}")
            //    println(s"Driver Stream Schema batchValidDF Before Union ${validDF.printSchema}")
            validDF = validDF.union(releasedDF.drop("receivedTimeStamp", "receivedBatch"))
            //   println(s"Driver Stream Schema batchValidDF After Union ${validDF.printSchema}")
            driverPersistDFIn = retainedDF
            //   println(s"Appending records to Driver state - before union batch ${batchID}")
            driverPersistDFIn = driverPersistDFIn.union(batchInvalid.withColumn("receivedTimeStamp", lit(current_timestamp)).withColumn("receivedBatch", lit(batchID)))
            //   println(s"Appending records to Driver state - after union batch ${batchID}")
            //    driverPersistDFIn.collect.map(x => s"Total retained in teamState ${x} in batch ${batchID}")
           // driverPersistDFIn.withColumn("driverPersistDFInTryTrue", lit("driverPersistDFInTryTrue")).show(false)

            val releasedTeamStateDF = stateAndStateDF.where("in_state.team_name is not null").selectExpr("in_state.*")
            val batchAndTeamState=teamPersistDFOut.as("state_out").join(validDF.as("released_in"),$"state_out.team_id"===$"released_in.teamId","left")
            batchAndTeamState.withColumn("batchAndTeamState",lit("batchAndTeamState")).show(false)

        //    val retainedTeamDF = teamPersistDFOut.except(releasedTeamDF)
            val retainedTeamDF = batchAndTeamState.filter("released_in.teamId is null").selectExpr("state_out.*")
              // teamPersistDFOut.as("state_out").join(releasedTeamStateDF.as("released_in"),$"state_out.team_id"===$"released_in.team_id","left").where("released_in.team_id is null").selectExpr("state_out.*")
            val releasedTeamVsValidDF = batchAndTeamState.where("released_in.teamId is not null").selectExpr("state_out.*")//.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag"))


            teamPersistDFOut = retainedTeamDF
            releasedTeamVsValidDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
            releasedTeamVsValidDF.drop("receivedTimeStamp", "receivedBatch").withColumn("releasedTeamInDriverState", lit("releasedTeamInDriverState")).show(false)
            println(s"printSchema of released team in driver ${releasedTeamVsValidDF.printSchema}")

            updateTeamTable(inputMap,releasedTeamVsValidDF)
            writeToTable(releasedTeamVsValidDF.drop("receivedBatch","receivedTimeStamp")/*.select(col("teamId").as("team_id"),col("teamName").as("team_name"),col("checkFlag").as("team_valid_flag"))*/,inputMap,inputMap("teamTableName"))
       // write released records into team table, update delts and insert

        }

        driverPersistDFOut = driverPersistDFIn

        validDF.withColumn("totalValidDriver", lit("totalValidDriver")).show(false)
        updateDriverTable(inputMap,validDF)
        println(s"printSchema of valid driver ${validDF.printSchema}")

        writeToTable(validDF.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")),inputMap,inputMap("driverTableName"))

        // write released records into driver table, update delts and insert
        //    writeToTable(validDF.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")),inputMap,inputMap("driverTableName"))
      }
    ).trigger(Trigger.ProcessingTime(10)).start

    spark.streams.awaitAnyTermination

    // working fine
    // same like combined resolving. here only state lookup. so the parent record needs to be sent again
    // spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.23 --class org.controller.persistingInsideJob.combinedResolvingStateAlone --num-executors 2 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 2 --deploy-mode client --master local[*] --conf 'spark.driver.extraJavaOptions=-DXmx=512m' --conf 'spark.driver.extraJavaOptions=-DXms=64m' /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServer=localhost:9092,localhost:9093,localhost:9094 topics=driverInfoTopic,teamInfoTopic driverStreamCheckpoint=hdfs://localhost:8020/user/raptor/streams/tstDriver/ teamStreamCheckpoint=hdfs://localhost:8020/user/raptor/streams/tstTeam/ offsetForTopic=latest driverMYSQL="com.mysql.cj.jdbc.Driver" username=raptor password= urlJDBC="jdbc:mysql://localhost:3306/testPersist" databaseName=testPersist teamTableName=team_info driverTableName=driver_info stateExpiry="INTERVAL 15 MINUTE"
  }

}
