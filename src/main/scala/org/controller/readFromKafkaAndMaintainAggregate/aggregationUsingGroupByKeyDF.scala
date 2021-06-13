/*
package org.controller.readFromKafkaAndMaintainAggregate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.SparkOpener
// import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}

object aggregationUsingGroupByKeyDF extends SparkOpener{
//   case class messageCaseClass(microService:String,page:String,eventDate:java.sql.Date,receivedTimestamp:java.sql.Timestamp)
  val spark=SparkSessionLoc()
  import spark.implicits._
  val structForMessage=new StructType(Array(StructField("microService",StringType,true),StructField("page",StringType,true),StructField("eventDate",StringType,true),StructField("receivedTimestamp",StringType,true)))
  val formatOfString= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  def timeStampConverter(x:String)=  Try{formatOfString.parse(x)} match {
  case Success(x) => new  Timestamp(x.getTime)
  case Failure(x) => null
}

  def main(args:Array[String]):Unit = {
  val inputMap=collection.mutable.Map[String,String]()
  for (arg <- args)
  inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

  val readSteamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topic")).option("offsets",inputMap("offset")).load.selectExpr("cast (value as string) value").select(from_json(col("value"),structForMessage).as("tmpParsed")).select("tmpParsed.*").selectExpr("cast (microService as string ) microService","cast (page as string ) page","cast (eventDate as date) eventDate","get_timestamp(receivedTimestamp) receivedTimestamp")

  readSteamDF.writeStream.format("console").option("checkpointLocation",inputMap("checkPoint")).foreachBatch(
  (df:DataFrame,batchId:Long) => batchFun(df,batchId,inputMap)
  ).queryName("groupByDS").start

  spark.streams.awaitAnyTermination
}
  // Seq((0L,"tmp")).toDF("batch_id","micro_service").write.partitionBy("batch_id").saveAsTable("temp_db.batch_micro_service_stats")

  def mapGroupsFunction(microServiceKey:String,batchId:Long,inputMap:collection.mutable.Map[String,String])=  spark.sql(s"insert into ${inputMap("hiveSchema")}.${inputMap("hiveStatsTable")} partition(batch_id='${batchId.toLong}') values('${microServiceKey}') ")

  // val tempDF= Seq((microServiceKey,batchId,"aggregationUsingGroupByKey"))


  def batchFun(df:DataFrame,batchId:Long,inputMap:collection.mutable.Map[String,String]) ={
    import spark.implicits._
    df.groupByKey("microService").mapGroups((key,groupedRows)=> {mapGroupsFunction(key,batchId,inputMap)
  groupedRows.toSeq}).flatMap(x => x).write.format("append").insertInto(s"${inputMap("hiveSchema")}.${inputMap("hiveStatsTable")}")

  // df.toDF.groupByKey(col(""))
}
  /*CREATE TABLE `temp_db`.`batch_micro_service_stats` (
  `micro_service` STRING,
  `batch_id` BIGINT)
USING parquet
PARTITIONED BY (batch_id)
location
'hdfs://localhost:8020/user/raptor/batch_micro_service_stats/';*/

}*/
