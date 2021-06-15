package org.controller.readFromKafkaAndMaintainAggregate

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.SparkOpener
import org.apache.spark.sql.Dataset

import java.sql.DriverManager
import java.util.Properties
// import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}

object aggregationUsingGroupByKey extends SparkOpener{
 case class messageCaseClass(microService:String,page:String,eventDate:java.sql.Date,receivedTimestamp:java.sql.Timestamp)
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  val structForMessage=new StructType(Array(StructField("microService",StringType,true),StructField("page",StringType,true),StructField("eventDate",StringType,true),StructField("receivedTimestamp",StringType,true)))
  val formatOfString= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

  def timeStampConverter(x:String)=  Try{formatOfString.parse(x)} match {
    case Success(x) => new  Timestamp(x.getTime)
    case Failure(x) => null
  }

  spark.udf.register("get_timestamp",timeStampConverter(_:String))
  def main(args:Array[String]):Unit = {
    val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    val readSteamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topic")).option("offsets",inputMap("offset")).load.selectExpr("cast (value as string) value").select(from_json(col("value"),structForMessage).as("tmpParsed")).select("tmpParsed.*").selectExpr("cast (microService as string ) microService","cast (page as string ) page","cast (eventDate as date) eventDate","get_timestamp(receivedTimestamp) receivedTimestamp").as[messageCaseClass]

    readSteamDF.writeStream.format("console").outputMode("update").option("checkpointLocation",inputMap("checkPoint")).foreachBatch(
      (df:Dataset[messageCaseClass],batchId:Long) => batchFun(df,batchId,inputMap)
    ).queryName("groupByDS").start

    spark.streams.awaitAnyTermination
  }
 // Seq((0L,"tmp")).toDF("batch_id","micro_service").write.partitionBy("batch_id").saveAsTable("temp_db.batch_micro_service_stats")

  // if null pointer comes try it over a mysql table
  def mapGroupsFunction(microServiceKey:String,batchId:Long,inputMap:collection.mutable.Map[String,String])= {
    println(s"inside mapGroupsFunction")
    val insertQuery=s"insert into ${inputMap("schemaName")}.${inputMap("hiveStatsTable")}(micro_service,batch_id) values ('${microServiceKey}','${batchId}')"
    println(s"inside insertQuery - ${insertQuery}")
    Class.forName(inputMap("driver"))
    val props=new Properties
    props.put("user",inputMap("user"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("jdbcURL"))
    val connection=DriverManager.getConnection(props.getProperty("url"),props)
    val insertStatement=connection.prepareStatement(insertQuery).executeUpdate
    connection.close

 //   spark.sql(s"insert into ${inputMap("hiveSchema")}.${inputMap("hiveStatsTable")} partition(batch_id='${batchId.toLong}') values('${microServiceKey}') ")
  }
  // val tempDF= Seq((microServiceKey,batchId,"aggregationUsingGroupByKey"))

// look out you have called spark inside map groups, usually calling action inside transformation will not work
  def batchFun(df:Dataset[messageCaseClass],batchId:Long,inputMap:collection.mutable.Map[String,String]) ={
    df.groupByKey(_.microService).mapGroups((key,groupedRows)=> {mapGroupsFunction(key,batchId,inputMap)
      groupedRows.toSeq}).flatMap(x => x).selectExpr("microService as micro_service","page","eventDate as event_date","receivedTimestamp as received_timestamp").withColumn("batch_id",lit(batchId)).write.mode("append").insertInto(s"${inputMap("hiveSchema")}.${inputMap("hiveBronzeTable")}")

   // df.toDF.groupByKey(col(""))
  }
  /*

what are all the microservice which came in, in that batch will be persisted to batch_micro_service_stats

  {"microService":"login","page":"login","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.214"}
{"microService":"login","page":"login","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"register","eventDate":"2020-08-02","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"forgot password","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"login","eventDate":"2020-08-02","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"login","page":"register","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}
{"microService":"order","page":"add-to-cart","eventDate":"2020-08-01","receivedTimestamp":"2020-08-01 23:45:98.234"}

CREATE TABLE `temp_db`.`batch_micro_service_stats` (
  `micro_service` STRING,
  `batch_id` BIGINT)
USING parquet
PARTITIONED BY (batch_id)
location
'hdfs://localhost:8020/user/raptor/batch_micro_service_stats/';

used mysql table. because of null pointer exception
 this groups by key using micro service name and enters what microservice came in every batch,. in hive could have partitioned, but as we use
 map groups couldn't use hive

CREATE TABLE testPersist.batch_micro_service_stats ( micro_service varchar(50), batch_id long);

spark-submit --class org.controller.readFromKafkaAndMaintainAggregate.aggregationUsingGroupByKey --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.25 --conf spark.sql.warehouse.dir=/user/raptor/tmp/hive/warehouse2/ --num-executors 2 --executor-cores 2 --driver-memory 1g --driver-cores 2 --executor-memory 1g --conf spark.sql.shuffle.partitions=4 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  bootstrapServer="localhost:9091,localhost:9092,localhost:9093" topic="tmp.cool.data" offset="latest" checkPoint="hdfs://localhost:8020/user/raptor/checkpointLocation/tmpcheck/" jdbcURL="jdbc:mysql://localhost:3306/testPersist?user=raptor&password=" password= user=raptor driver="com.mysql.jdbc.Driver" schemaName=testPersist totalTableName=total_count dailyTableName=day_wise_count hiveSchema=temp_db hiveBronzeTable=page_click_events_bronze totalColumns="micro_service,page,hit_count" dailyColumns="micro_service,page,event_date,hit_count" hiveStatsTable=batch_micro_service_stats
*/


}
