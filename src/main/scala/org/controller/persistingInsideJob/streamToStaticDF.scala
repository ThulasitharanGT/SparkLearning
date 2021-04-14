package org.controller.persistingInsideJob

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.controller.persistingInsideJob.jobHelper.{argsToMapConvert, getExistingRecords, getJDBCConnection, idGetterDriver, idListToStringManipulator, streamingDFToStaticDF, writeToTable}
import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.persistingInsideJob.jobConstantsPersist._

object streamToStaticDF extends SparkOpener{

  case class statsTable(driver_id:String,driver_name:String,total_poles:String,total_wins:String,total_laps_lead:String,recorded_date:java.sql.Date)

  val spark=SparkSessionLoc()
  def main(args:Array[String]):Unit={
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val inputMap=argsToMapConvert(args)
    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("kafkaBootstrapServers")).option("subscribe",inputMap("topic")).option("offsets",inputMap("startingOffsets")).load.select(col("value").cast(StringType))
    val requiredDF=readStreamDF.select(from_json(col("value"),schemaOfMessage).as("parsedEvent")).selectExpr("parsedEvent.*").filter(col("eventInfo")===lit(driverStatsEvent)).select(from_json(col("eventData"),driverStatsSchema).as("driverStats")).selectExpr("driverStats.*").select(col("driverName").as("driver_id"),col("driverId").as("driver_name"),col("totalPoles").as("total_poles"),col("totalWins").as("total_wins"),col("totalLapsLead").as("total_laps_lead"),col("recordedDate").as("recorded_date")).as[statsTable]
    val staticAndStreamingDF=streamingDFToStaticDF(requiredDF,spark)
    val columnNamesSeq="driver_id, driver_name, total_poles, total_wins, total_laps_lead, recorded_date".split(",").map(_.trim)
    val staticDF=staticAndStreamingDF._1 match {case  null => Seq(("","","","","",new java.sql.Date(System.currentTimeMillis))).toDF(columnNamesSeq:_*).as[statsTable] case _ => staticAndStreamingDF._1.as[statsTable] }
    val streamingDF=staticAndStreamingDF._2
    val driverIds=idGetterDriver(streamingDF.toDF)
    val currentStateDF=getExistingRecords(inputMap,spark,s"select * from ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} where driver_id in (${idListToStringManipulator(driverIds)})")
    val recordsToBeInvalidated=currentStateDF.as("hist").join(staticDF.as("curr"),col("curr.driver_id")===col("hist.driver_id") && to_date(col("curr.recorded_date"))===to_date(col("hist.recorded_date"),"yyyy-MM-dd"),"left").where("curr.driver_id is null").selectExpr("hist.*")
    currentStateDF.show(false)
    recordsToBeInvalidated.map(x=>invalidateInfo(x,inputMap)).show
    streamingDF.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("checkpointLocation")).foreachBatch(
      (batchDF:Dataset[statsTable],batchId:Long) =>{
        println(s"batch DF Schema ${batchDF.toDF.printSchema}")
        writeToTable(batchDF.toDF,inputMap,s"${inputMap("databaseName")}.${inputMap("driverStatsTableName")}")
      }
    ).start
    spark.streams.awaitAnyTermination
  }
/*
 create table testPersist.driver_stats(
driver_id VARCHAR(50) ,
driver_name VARCHAR(50) ,
total_poles VARCHAR(50) ,
total_wins VARCHAR(50) ,
total_laps_lead VARCHAR(50) ,
recorded_date DATE );


*/

  def invalidateInfo(row:Row,inputMap:collection.mutable.Map[String,String])={
    val statsTableRecord= statsTable(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),row.getDate(5))
    val conn=getJDBCConnection(inputMap)
    val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} set ${inputMap("totalTotalPolesColumn")}='0' ,${inputMap("totalTotalWinsColumn")}='0',${inputMap("totalTotalLapsLeadColumn")}='0' where driver_id='${statsTableRecord.driver_id}' and recorded_date='${statsTableRecord.recorded_date}'"
    val updateStatement=conn.prepareStatement(updateQuery)
    val numRecordsAffected=updateStatement.executeUpdate
    conn.close
    numRecordsAffected
    //totalTotalPolesColumn=total_poles totalTotalWinsColumn=total_wins  totalTotalLapsLeadColumn=total_laps_lead driverStatsTableName=
    // total_poles:String,total_wins:String,total_laps_lead
  }
}
