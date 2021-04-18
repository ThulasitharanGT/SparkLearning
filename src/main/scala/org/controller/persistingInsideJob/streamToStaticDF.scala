package org.controller.persistingInsideJob

import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, Row, SparkSession}
import org.controller.persistingInsideJob.jobHelper.{argsToMapConvert, getExistingRecords, getJDBCConnection, idGetterDriver, idListToStringManipulator, streamingDFToStaticDF, writeToTable}
import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.persistingInsideJob.jobConstantsPersist._

object streamToStaticDF extends SparkOpener{
  val columnNamesSeq="driver_id, driver_name, total_poles, total_wins, total_laps_lead, recorded_date".split(",").map(_.trim)

  case class statsTable(driver_id:String,driver_name:String,total_poles:String,total_wins:String,total_laps_lead:String,recorded_date:java.sql.Date)
  case class statsTableWithUpdatedInfo(driver_id:String,driver_name:String,total_poles:String,total_wins:String,total_laps_lead:String,recorded_date:java.sql.Date,numRecordsAffected:Int)

  val spark=SparkSessionLoc()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit={
    val inputMap=argsToMapConvert(args)
    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("kafkaBootstrapServers")).option("subscribe",inputMap("topic")).option("offsets",inputMap("startingOffsets")).load.select(col("value").cast(StringType))
    val requiredDF=readStreamDF.select(from_json(col("value"),schemaOfMessage).as("parsedEvent")).selectExpr("parsedEvent.*").filter(col("eventInfo")===lit(driverStatsEvent)).select(from_json(col("eventData"),driverStatsSchema).as("driverStats")).selectExpr("driverStats.*").select(col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("totalPoles").as("total_poles"),col("totalWins").as("total_wins"),col("totalLapsLead").as("total_laps_lead"),col("recordedDate").as("recorded_date")).as[statsTable]
//    val staticAndStreamingDF=streamingDFToStaticDF(requiredDF,spark)
    println(s"printSchema root ${requiredDF.printSchema}")
  val streamingDF=invalidateFunction(requiredDF,inputMap/*,spark*/)

/*    val staticDF=staticAndStreamingDF._1 match {case  null => Seq(("","","","","",new java.sql.Date(System.currentTimeMillis))).toDF(columnNamesSeq:_*).as[statsTable] case _ => staticAndStreamingDF._1.as[statsTable] }
    val streamingDF=staticAndStreamingDF._2*/

/*
    currentStateDF.withColumn("currentStateDF",lit("currentStateDF")).writeStream.format("console").option("truncate","false").option("checkpointLocation",inputMap("checkpointLocationCurrentState")).start
    val invalidateRecordsStream=recordsToBeInvalidated.map(x=>invalidateInfo(x,inputMap)).writeStream.format("console").option("truncate","false").option("checkpointLocation",inputMap("checkpointLocationInvalidate")).start
*/

    requiredDF.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("checkpointLocationInvalidate")).foreachBatch(
      (batchDF:Dataset[statsTable],batchId:Long) =>
        batchDF.toDF.withColumn(s"batchDF_console_${batchId}",lit(s"batchDF_console_${batchId}")).show(false)

    ).start

   //   .option("truncate","false").start

    streamingDF.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("checkpointLocation")).foreachBatch(
      (batchDF:Dataset[statsTable],batchId:Long) =>{
        println(s"batch DF Schema streamingDF._1 ${batchDF.toDF.printSchema}")
       // println(s"batch DF tableName ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} ")
        batchDF.toDF.withColumn(s"batchDF_${batchId}",lit(s"batchDF_${batchId}")).show(false)
        val finalBatchDF=batchDF.toDF match {case null => Seq(("","","","","",new java.sql.Date(System.currentTimeMillis))).toDF(columnNamesSeq:_*) case value => value }
        finalBatchDF.withColumn(s"finalBatchDF_${batchId}",lit(s"finalBatchDF_${batchId}")).show(false)
        writeToTable(finalBatchDF,inputMap,/*s"${inputMap("databaseName")}.*/s"${inputMap("driverStatsTableName")}")
      }
    ).start




    /*.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("checkpointLocationInvalidate")).foreachBatch(
      (batchDF:DataFrame,batchId:Long) =>{
        println(s"invaliate DF Schema ${batchDF.toDF.printSchema}")
       batchDF.map(invalidateInfo(_,inputMap)).show(false)  //.filter("recorded_date is null").write.mode("overwrite")
      //  writeToTable(batchDF.map(invalidateInfo(_,inputMap)).filter("recorded_date is null").toDF,inputMap,s"${inputMap("databaseName")}.${inputMap("driverStatsTableNameTmp")}")
      }
    ).start*/

/*    streamingDF._2.writeStream.foreach(new ForeachWriter[Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = true

      override def process(value: Row): Unit = {

      }

      override def close(errorOrNull: Throwable): Unit = Unit
    }).start*/


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


{"eventInfo":"driverStatsEvent","eventData":"{\"driverName\":\"Senna\",\"driverId\":\"D001\",\"totalPoles\":\"65\",\"totalWins\":\"41\",\"totalLapsLead\":\"2000\",\"recordedDate\":\"2021-05-01\"}"}

{"eventInfo":"driverStatsEvent","eventData":"{\"driverName\":\"Senna\",\"driverId\":\"D001\",\"totalPoles\":\"65\",\"totalWins\":\"41\",\"totalLapsLead\":\"2000\",\"recordedDate\":\"2021-05-05\"}"}
{"eventInfo":"driverStatsEvent","eventData":"{\"driverName\":\"Senna\",\"driverId\":\"D001\",\"totalPoles\":\"65\",\"totalWins\":\"41\",\"totalLapsLead\":\"2000\",\"recordedDate\":\"2021-05-07\"}"}
{"eventInfo":"driverStatsEvent","eventData":"{\"driverName\":\"Max\",\"driverId\":\"D002\",\"totalPoles\":\"6\",\"totalWins\":\"11\",\"totalLapsLead\":\"100\",\"recordedDate\":\"2021-05-07\"}"}

///////////////////////////// this is not working the batch df inside the streaming df mapping and generation is only execute once. We need to go with map on streaming df to get the job done.
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,mysql:mysql-connector-java:8.0.23 --class org.controller.persistingInsideJob.streamToStaticDF --num-executors 2 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 2 --deploy-mode client --master local[*] --conf 'spark.driver.extraJavaOptions=-DXmx=512m' --conf 'spark.driver.extraJavaOptions=-DXms=64m' /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaBootstrapServers=localhost:9092,localhost:9093,localhost:9094 topic=driverStatsTopic checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tstDriverStats/  startingOffsets=latest driverMYSQL="com.mysql.cj.jdbc.Driver" username=raptor password= urlJDBC="jdbc:mysql://localhost:3306/testPersist" databaseName=testPersist teamTableName=team_info driverTableName=driver_info stateExpiry="INTERVAL 15 MINUTE" totalTotalPolesColumn=total_poles totalTotalWinsColumn=total_wins  totalTotalLapsLeadColumn=total_laps_lead driverStatsTableName=driver_stats checkpointLocationInvalidate=hdfs://localhost:8020/user/raptor/streams/tstDriverInvalidate/ driverStatsTableNameTmp=driver_archive

*/

  def invalidateInfo(row:Row,inputMap:collection.mutable.Map[String,String])={
    val statsTableRecord= statsTable(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),row.getDate(5))
    val conn=getJDBCConnection(inputMap)
    val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} set ${inputMap("totalTotalPolesColumn")}='0' ,${inputMap("totalTotalWinsColumn")}='0',${inputMap("totalTotalLapsLeadColumn")}='0' where driver_id='${statsTableRecord.driver_id}' and recorded_date='${statsTableRecord.recorded_date}'"
    val updateStatement=conn.prepareStatement(updateQuery)
    val numRecordsAffected=updateStatement.executeUpdate
    conn.close
    statsTableWithUpdatedInfo(row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4),row.getDate(5),numRecordsAffected)
    //totalTotalPolesColumn=total_poles totalTotalWinsColumn=total_wins  totalTotalLapsLeadColumn=total_laps_lead driverStatsTableName=
    // total_poles:String,total_wins:String,total_laps_lead
    //{"eventInfo":"driverStatsEvent","{\"driverName\":\"Senna\",\"driverId\":\"D001\",\"totalPoles\":\"65\",\"totalWins\":\"41\",\"totalLapsLead\":\"2000\",\"recordedDate\":\"2020-04-01\"}"}

  }


  def invalidateFunction(df:Dataset[statsTable],inputMap:collection.mutable.Map[String,String]/*,spark:SparkSession*/)={
  //  import spark.implicits._
    println(s"invalidateFunction :: start")
    var staticDF:DataFrame=null
    val streamingDFFinal=df.map( x =>                           // converting this streaming DF to a batch DF
      staticDF match {
        case null =>
          println(s"invalidateFunction :: staticDF null x ${x}")
          staticDF = Seq(x).toDF
          x
        case _ =>
          println(s"invalidateFunction :: staticDF union x ${x}")
          staticDF = staticDF.union(Seq(x).toDF)
          x
      }
    )
    val driverIdAndStreamingDF=idGetterDriverCaseClass(streamingDFFinal/*,spark*/)
    println(s"invalidateFunction :: driverIds ${driverIdAndStreamingDF._1}")
    val finalStaticDF=staticDF match {case  null => Seq(("","","","","",new java.sql.Date(System.currentTimeMillis))).toDF(columnNamesSeq:_*).as[statsTable] case _ => staticDF.toDF(columnNamesSeq:_*).as[statsTable] }
    println(s"invalidateFunction :: finalStaticDF ${finalStaticDF.show(false)}")

  val currentStateDF=getExistingRecords(inputMap,spark,s"(select * from ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} where driver_id in (${idListToStringManipulator(driverIdAndStreamingDF._1.toList)}) )a")
    println(s"invalidateFunction :: currentStateDF ${currentStateDF.show(false)}")

    val recordsToBeInvalidated=currentStateDF.as("hist").join(finalStaticDF.as("curr"),col("curr.driver_id")===col("hist.driver_id") && to_date(col("curr.recorded_date"))===to_date(col("hist.recorded_date"),"yyyy-MM-dd"),"left").where("curr.driver_id is null").selectExpr("hist.*")
    println(s"invalidateFunction :: recordsToBeInvalidated")

    recordsToBeInvalidated.map(invalidateInfo(_,inputMap)).withColumn("recordsToBeInvalidated",lit("recordsToBeInvalidated")).show(false)   // invalidating records acion triggers the dag to be computed
    driverIdAndStreamingDF._2 //,recordsToBeInvalidated)
  }

  def invalidateInfo(row:statsTable,inputMap:collection.mutable.Map[String,String])={
    val conn=getJDBCConnection(inputMap)
    //  val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} set ${inputMap("totalTotalPolesColumn")}='0' ,${inputMap("totalTotalWinsColumn")}='0',${inputMap("totalTotalLapsLeadColumn")}='0' where driver_id='${row.driver_id}' and recorded_date!='${row.recorded_date}'"
    val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("driverStatsTableName")} set ${inputMap("totalTotalPolesColumn")}='0' ,${inputMap("totalTotalWinsColumn")}='0',${inputMap("totalTotalLapsLeadColumn")}='0' where driver_id='${row.driver_id}' and recorded_date>=current_date()"  /*'${row.recorded_date}'*/ // condition change for checking
    println(updateQuery)
    val updateStatement=conn.prepareStatement(updateQuery)
    val numRecordsAffected=updateStatement.executeUpdate
    conn.close
    statsTableWithUpdatedInfo(row.driver_id,row.driver_name,row.total_poles,row.total_wins,row.total_laps_lead,row.recorded_date,numRecordsAffected)
  }

  def idGetterDriverCaseClass(df:Dataset[statsTable]/*,spark:SparkSession*/)= {
 //   import spark.implicits._
    println(s"inside idGetterDriverCaseClass")
    var idSeq=Seq("")
    var numIter=1
    //idGetterCaseClass()
    val tempDF=df.map(x =>
      numIter match {
        case value if value ==1 =>
          idSeq=Seq(x.driver_id)
          numIter=numIter+1
          x
        case _ =>
          idSeq=idSeq++Seq(x.driver_id)
          numIter=numIter+1
          x
      }
    )
    println(s"inside idGetterDriverCaseClass idSeq.filter(_.trim.size>0) ${idSeq.filter(_.trim.size>0)}")
    (idSeq.filter(_.trim.size>0),tempDF)
  }

}
