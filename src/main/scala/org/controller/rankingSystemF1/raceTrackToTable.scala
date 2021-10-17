package org.controller.rankingSystemF1

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StringType
import org.controller.rankingSystemF1.utils.{getInputMap, getJDBCConnection, raceTrackInfo, raceTrackInfoWithResult, schemaOfOuterLayer, schemaOfRaceTrack, sendMessageToKafka}
import org.util.SparkOpener

object raceTrackToTable extends SparkOpener{

  val spark=SparkSessionLoc()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
  def main(args:Array[String]):Unit={
    val inputMap=getInputMap(args)

    spark.readStream.format("kafka").option("subscribe",inputMap("topic"))
      .option("kafka.bootstrap.servers",inputMap("bootStrapServer")).option("startingOffsets",inputMap("startingOffsets")).load
      .select(from_json(col("value").cast(StringType),schemaOfOuterLayer).as("valueStruct")).select("valueStruct.*")
      .filter("messageType ='raceTrackInfo' ").drop("messageType")
      .select(from_json(col("incomingMessage").cast(StringType),schemaOfRaceTrack).as("valueStruct"),col("messageTimestamp")
      ).selectExpr("valueStruct.*", "cast(messageTimestamp as timestamp) messageTimestamp")
      .writeStream.format("console")
      .option("checkpointLocation",inputMap("checkPointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)=>{
        forEachBatchFunction(df,batchId,inputMap)
      }).start
    spark.streams.awaitAnyTermination
  }

  def forEachBatchFunction(df:org.apache.spark.sql.DataFrame,batchID:Long,inputMap:collection.mutable.Map[String,String])={
    // df.dropDuplicates("raceTrackId","raceVenue").as[]

    df.withColumn("dupeFilter",row_number.over(Window.partitionBy("raceTrackId","raceVenue").orderBy(desc("messageTimestamp"))))
      .where("dupeFilter=1").drop("dupeFilter").as[raceTrackInfo].map(x=>{
       val conn=getJDBCConnection(inputMap)
      val (resultType,rowsAffected)=insertOrUpdate(conn,x, inputMap)
      conn.close
      sendMessageToKafka(s"""{"messageType":"triggerRaceInfo","messageTimestamp":"${new java.sql.Timestamp(System.currentTimeMillis)}","incomingMessage":"${x.toKafkaPayload}"}""",inputMap)
      raceTrackInfoWithResult(x.raceTrackId,x.raceVenue,x.messageTimestamp,resultType,rowsAffected)
    }
    ).show(false)
  }

  def insertOrUpdate(conn:java.sql.Connection,record:raceTrackInfo,inputMap:collection.mutable.Map[String,String]) = doesRecordExists(conn,record, inputMap) match {
    case value if value == true =>("update",updateRecord(conn,record, inputMap))
    case  value if value == false => ("insert",insertRecord(conn,record, inputMap))
  }

  def doesRecordExists(conn:java.sql.Connection,record:raceTrackInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("raceTrackInfoTable")} where race_track_id='${record.raceTrackId}'").executeQuery.next

  def updateRecord(conn:java.sql.Connection,record:raceTrackInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("raceTrackInfoTable")} set incoming_timestamp='${record.messageTimestamp}' where race_track_id='${record.raceTrackId}'").executeUpdate

  def insertRecord(conn:java.sql.Connection,record:raceTrackInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("raceTrackInfoTable")}(race_track_id, race_venue, incoming_timestamp) values ('${record.raceTrackId}','${record.raceVenue}','${record.messageTimestamp}')").executeUpdate
}
