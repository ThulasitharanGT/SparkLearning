package org.controller.persistingInsideJob

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.SparkOpener
import jobConstantsPersist._
import jobHelper._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object stage2Job extends SparkOpener{
  def main(args:Array[String]):Unit ={
    val spark=SparkSessionLoc()
    val inputMap=argsToMapConvert(args)
    spark.sparkContext.setLogLevel("ERROR")
    /*
create table testPersist.driver_info(team_id VARCHAR(50),
driver_id VARCHAR(50) ,
driver_name VARCHAR(50) ,
active_flag VARCHAR(50) ,
insert_timestamp TIMESTAMP DEFAULT current_timestamp(),
delete_timestamp TIMESTAMP DEFAULT NULL);

teamTableName
driverTableName

 */
    val driverSchema=StructType(Array(StructField("teamId",StringType,true),StructField("driverId",StringType,true),StructField("driverName",StringType,true),StructField("activeFlag",StringType,true)))

    var tmpDataFrameOut:DataFrame=null // stateDF
    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topic")).option("offset",inputMap("offsetForTopic")).load.select(from_json(col("value").cast(StringType),schemaOfMessage).as("eventConverted")).selectExpr("eventConverted.*").filter(s"eventInfo = '${driverEvent}'").select(from_json(col("eventData").cast(StringType),driverSchema).as("driverCols")).selectExpr("driverCols.*")

    readStreamDF.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("checkpointLocation")).foreachBatch({
      (batchDF:DataFrame,batchID:Long) =>
        var tmpDataFrameIn=tmpDataFrameOut // using another DF inside to persist because if We use same DF inside and Outside records are getting dropped abnormally, suspect this is because of distributed behaviour of spark
        batchDF.withColumn("batchTmp",lit("batchTmp")).show(false)
        val teamIDListBatch=idGetterTeam(batchDF)
        val teamIDListState= Try{tmpDataFrameIn.count}.isSuccess match { case true => idGetterTeam(tmpDataFrameIn) case false => List() }
        val idListForQuery=idListToStringManipulator(teamIDListBatch++teamIDListState)  // includes db lookup for state, so taking id's of records in state
        // println(s"id's for whereCondition ${idListForQuery}")
        inputMap.put("whereCondition","and delete_timestamp is NULL")
        inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("teamTableName")} where team_id in (${idListForQuery}) ${Try{inputMap("whereCondition")}.isSuccess match {case true => inputMap("whereCondition") case false =>""}}) a")
        val existingRecordsTeamDF=getExistingRecords(inputMap,spark)
        var validRecordsDF:DataFrame=null  // this is to persist all the records which has satisfied the dependency. (ie) released from state and
        Try{tmpDataFrameIn.count}.isSuccess match { // first time the stateDF will be empty
          case false =>
            println("No records in State")
          case true =>
     //       println("Records are in State")
            // expiry of stateDF. This will expire according to the expression given through CLI
            val tmpDFManipulated=tmpDataFrameIn.withColumn("presentTime",current_timestamp).withColumn("minusTimeStamp",col("presentTime")- expr(inputMap("stateExpiry"))).withColumn("plusTimeStamp",col("receivedTimeStamp")+ expr(inputMap("stateExpiry")))
            tmpDFManipulated.withColumn("tmpManipulated",lit("tmpManipulated")).show(false)
            val tmpDataFrameExpired=tmpDFManipulated.where("receivedTimeStamp <= minusTimeStamp")
            val tmpDataFrameRetained=tmpDFManipulated.where("plusTimeStamp >= presentTime")
            tmpDataFrameExpired.collect.map(x => println(s"Expired from state ${x} in batch ${batchID}"))
//            tmpDataFrameRetained.withColumn("tmpRetained",lit("tmpRetained")).show(false)
            tmpDataFrameRetained.collect.map(x => println(s"Retained in state ${x} in batch ${batchID}"))
          // only retained DF is being used to resolve dependency in DB
            tmpDataFrameIn=tmpDataFrameRetained.drop("presentTime","minusTimeStamp","plusTimeStamp"/*,"receivedTimeStamp","receivedBatchId"*/)
            // release of stateDF
            val tmpJoinStateDF=tmpDataFrameIn.as("state").join(existingRecordsTeamDF.as("in_db"),col("state.teamId")===col("in_db.team_id"),"left")
       //     tmpJoinStateDF.withColumn("tmpStateJoin",lit("tmpStateJoin")).show(false)
            val validStateRecords=tmpJoinStateDF.filter("in_db.team_id is not null")
            val invalidStateRecords=tmpJoinStateDF.filter("in_db.team_id is null")
         // taking the records which have parent records in DB
            validRecordsDF=validStateRecords.select("state.*").drop("receivedTimeStamp","receivedBatchId")
          // retaining un resolved reords
            tmpDataFrameIn=invalidStateRecords.select("state.*")
            invalidStateRecords.collect.map(x => println(s"Un resolved records ${x} in batch ${batchID}"))
            validStateRecords.collect.map(x => println(s"Released from state ${x} in batch ${batchID}"))
        }
        println("before batch join")
        val tmpJoinBatchDF=batchDF.as("batch").join(existingRecordsTeamDF.as("in_db"),col("batch.teamId")===col("in_db.team_id"),"left")
  //      tmpJoinBatchDF.withColumn("tmpBatchJoin",lit("tmpBatchJoin")).show(false)
        val validIncomingRecordsBatch=tmpJoinBatchDF.filter("in_db.team_id is not null")
        val invalidIncomingRecordsBatch=tmpJoinBatchDF.filter("in_db.team_id is null")
        invalidIncomingRecordsBatch.collect.map(x => println(s"Un resolved record from DB ${x} in batch ${batchID}"))
        validIncomingRecordsBatch.collect.map(x => println(s"Resolved record from DB ${x} in batch ${batchID}"))
        Try{validRecordsDF.count}.isSuccess match { // if no records in state this might be null
          case false =>
            println("No valid records ") // adding valid records from batch
            validRecordsDF=validIncomingRecordsBatch.select("batch.*")
          case true =>
            println("Valid records ") // adding valid records from batch along with released records from state
            validRecordsDF=validRecordsDF.union(validIncomingRecordsBatch.select("batch.*"))
        }
       println("Adding records to state ")
        Try{tmpDataFrameOut.count}.isSuccess  match { // in case if it is the first batch this might be null
          case false =>
            // if the DF is null in first batch just add unresolved batch records to state
          tmpDataFrameOut=invalidIncomingRecordsBatch.selectExpr("batch.*").withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatchId",lit(batchID))
         //   tmpDataFrameOut.withColumn("stateTmpFalse",lit("stateTmpFalse")).show(false)
            tmpDataFrameOut.collect.map(x => println(s"Records in state ${x} in batch ${batchID}"))
          case true =>
            tmpDataFrameIn.withColumn("stateTmpTrueBefore",lit("stateTmpTrueBefore")).show(false)
           // tmpDataFrame.printSchema
           // invalidIncomingRecordsBatch.selectExpr("batch.*").printSchema
            // if records are in state then add unresolved batch records along with it
            tmpDataFrameOut=tmpDataFrameIn.union(invalidIncomingRecordsBatch.selectExpr("batch.*").withColumn("receivedTimeStamp",lit(current_timestamp)).withColumn("receivedBatchId",lit(batchID)))
            tmpDataFrameOut.persist(StorageLevel.MEMORY_AND_DISK_SER)
            tmpDataFrameOut.collect.map(x => println(s"Records in state ${x} in batch ${batchID}"))
          //   tmpDataFrameOut.withColumn("stateTmpTrue",lit("stateTmpTrue")).show(false)
        }
       // validRecordsDF.withColumn("validTmp",lit("validTmp")).show(false)
        validRecordsDF.collect.map(x => println(s"All valid records ${x} in batch ${batchID}"))
        insertIntoDriverTable(validRecordsDF,spark,inputMap)
    }).trigger(Trigger.ProcessingTime(10)).start
    spark.streams.awaitAnyTermination
  }

  def idGetterDriver(df:DataFrame)=idGetter(df,1)
  def idGetterTeam(df:DataFrame)=idGetter(df,0)

  def insertIntoDriverTable(validRecordsDF:DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String])={
    val driverIDList=idGetterDriver(validRecordsDF)
    val teamIDList=idGetterTeam(validRecordsDF)
    inputMap.put("whereCondition","and delete_timestamp is NULL and active_flag='Y' ")
 //   inputMap.put("queryString",s"(select * from ${inputMap("databaseName")}.${inputMap("driverTableName")} where driver_id in (${driverIDList}) ${Try{inputMap("whereCondition")}.isSuccess match {case true => inputMap("whereCondition") case false =>""}}) a")
 //   val existingDriverIDInDB=getExistingRecords(inputMap,spark)
    println(s"Query String - select team_id,count(driver_id) as driver_count from ${inputMap("databaseName")}.${inputMap("driverTableName")} where team_id in (${idListToStringManipulator(teamIDList)}) ${Try{inputMap("whereCondition")}.isSuccess match {case true => inputMap("whereCondition") case false =>""}} group by team_id having count(driver_id) >4 ")
    inputMap.put("queryString",s"(select team_id,count(driver_id) as driver_count from ${inputMap("databaseName")}.${inputMap("driverTableName")} where team_id in (${idListToStringManipulator(teamIDList)}) ${Try{inputMap("whereCondition")}.isSuccess match {case true => inputMap("whereCondition") case false =>""}} group by team_id having count(driver_id) >4 ) a")
    val existingDriverIDInDBPerTeam=getExistingRecords(inputMap,spark)
    val exceededTeamIdList=idGetterTeam(existingDriverIDInDBPerTeam)
    val validFinalDF=validRecordsDF.filter(s"teamID not in (${idListToStringManipulator(exceededTeamIdList)})")
    val finalDriverIDList=idGetterDriver(validFinalDF)
    driverIDList.diff(finalDriverIDList) match
    {
      case value if value.size >0 =>  println(s"Excluded driver id's ${driverIDList.diff(finalDriverIDList)}")
      case _ => println(s"No driver id's excluded")
    }
    inputMap.put("sqlQuery",s"update ${inputMap("databaseName")}.${inputMap("driverTableName")} set active_flag ='N',delete_timestamp=current_timestamp() where driver_id in (${idListToStringManipulator(finalDriverIDList)}) and delete_timestamp is null")
    updateExistingValidRecords(inputMap)
    saveFinalResultToDB(validFinalDF,inputMap)
  }

  def saveFinalResultToDB(finalDF:DataFrame,inputMap:collection.mutable.Map[String,String])= finalDF.select(col("teamId").as("team_id"),col("driverId").as("driver_id"),col("driverName").as("driver_name"),col("activeFlag").as("active_flag")).write.mode("append").format("jdbc").option("driver",inputMap("driverMYSQL")).option("user",inputMap("username")).option("password",inputMap("password")).option("url",inputMap("urlJDBC")).option("dbtable",s"${inputMap("databaseName")}.${inputMap("driverTableName")}").save



}
