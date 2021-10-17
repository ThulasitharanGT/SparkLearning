
package org.controller.rankingSystemF1

import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.rankingSystemF1.utils.{driverInfo, driverInfoWithResult, getInputMap, getJDBCConnection, schemaOfDriver, schemaOfOuterLayer, sendMessageToKafka}

object driverToTable extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  def main(args:Array[String]):Unit={
    val inputMap=getInputMap(args)

    spark.readStream.format("kafka").option("subscribe",inputMap("topic"))
      .option("kafka.bootstrap.servers",inputMap("bootStrapServer")).option("startingOffsets",inputMap("startingOffsets"))
      .load
      .select(from_json(col("value").cast(StringType),schemaOfOuterLayer).as("valueStruct")).select("valueStruct.*")
      .filter("messageType ='driverInfo' ").drop("messageType")
      .select(from_json(col("incomingMessage").cast(StringType),schemaOfDriver).as("valueStruct"),col("messageTimestamp")
       ).selectExpr("valueStruct.*", "cast(messageTimestamp as timestamp) messageTimestamp")
      .writeStream.format("console")
      .option("checkpointLocation",inputMap("checkPointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)=>{
        forEachBatchFunction(df,batchId,inputMap)
      }).start

    spark.streams.awaitAnyTermination

  }
 //  import org.apache.kafka.common.serialization.StringSerializer

  def forEachBatchFunction(df:org.apache.spark.sql.DataFrame,batchId:Long,inputMap:collection.mutable.Map[String,String])={
    df.selectExpr("driverId","driverName","season","messageTimestamp").as[driverInfo].map(
      x=> {
        val conn=getJDBCConnection(inputMap)
        val (resultType,rowsAffected)=insertOrUpdateDriverTable(conn,x, inputMap)
        conn.close
        sendMessageToKafka(s"""{"messageType":"triggerDriverPoints","messageTimestamp":"${new java.sql.Timestamp(System.currentTimeMillis)}","incomingMessage":"${x.toKafkaPayload}"}""",inputMap)
        driverInfoWithResult(x.driverId,x.driverName,x.season,x.messageTimestamp,resultType,rowsAffected)
      }
    ).show(false)
  }

  def insertOrUpdateDriverTable(conn:java.sql.Connection,record:driverInfo,inputMap:collection.mutable.Map[String,String])
  =doesDriverRecordExists(conn,record, inputMap) match {
    case value if value == true => ("update",updateDriverRecord(conn,record, inputMap) )
    case value if value == false =>("insert",insertDriverRecord(conn,record, inputMap) )
  }

  def updateDriverRecord(conn:java.sql.Connection,record:driverInfo,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("driverInfoTable")} set incoming_timestamp='${record.messageTimestamp}' where driver_id='${record.driverId}' and season='${record.season}'").executeUpdate

  def insertDriverRecord(conn:java.sql.Connection,record:driverInfo,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"insert into ${inputMap("schemaName")}.${inputMap("driverInfoTable")} (driver_id,driver_name,season,incoming_timestamp)  values('${record.driverId}','${record.driverName}','${record.season}','${record.messageTimestamp}')").executeUpdate

  def doesDriverRecordExists(conn:java.sql.Connection,record:driverInfo,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"select * from ${inputMap("schemaName")}.${inputMap("driverInfoTable")} where driver_id='${record.driverId}' and season='${record.season}'").executeQuery.next
}



