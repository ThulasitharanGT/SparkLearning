package org.controller.samePointsAndDifferentTime

import io.delta.tables.DeltaTable
import org.constants.projectConstants
import org.util.SparkOpener
import org.util.readWriteUtil.readStreamFunction
import org.apache.spark.sql.functions._

//import java.text.SimpleDateFormat
//import java.util.Date
import sys.process._

object deltaStreamToUpdateTheFinalTable extends SparkOpener{
  val spark=SparkSessionLoc()
//  val dateFormat=new SimpleDateFormat("YYYY-MM-DD HH:mm:ss.SSS")

  def main(args:Array[String]) :Unit ={
  //  import spark.implicits._
    val inputMap = collection.mutable.Map[String, String]()
    for (arg <- args) {
      val keyPart = arg.split("=", 2)(0)
      val valPart = arg.split("=", 2)(1)
      inputMap.put(keyPart, valPart)
    }
    val checkpointLocation = inputMap("checkpointLocation")
    val clearCheckpointFlag = inputMap("clearCheckpointFlag")
    val persistPath = inputMap("persistPath")
    val inputPath = inputMap("inputPath")


    println(s"Checking if checkpoint exists in ${checkpointLocation}")
    val checkpointExists = s"hdfs dfs -ls ${checkpointLocation}" !; // run time error might occur if you fail to terminate the statement
    checkpointExists match {
      case value if value == 0 =>
        println(s"Checkpoint exists in ${checkpointLocation}")
        clearCheckpointFlag match {
          case value if value.toUpperCase == "Y" =>
            val checkpointClear = s"hdfs dfs -rm -r ${checkpointLocation}" !;
            checkpointClear match {
              case value if value == 0 =>
                println(s"Checkpoint cleared  in ${checkpointLocation}")
              case value if value != 0 =>
                println(s"Error in clearing checkpoint  ${checkpointLocation}")
            }
          case value if value.toUpperCase == "N" =>
            println(s"Checkpoint wont be cleared  in ${checkpointLocation}")
          case value =>
            println(s"Invalid option for clearCheckpointFlag - ${value}")
        }
      case value if value != 0 =>
        println(s"Checkpoint does not exists in ${checkpointLocation}")
    }
    inputMap.put(projectConstants.fileFormatArg,projectConstants.deltaFormat)
    inputMap.put(projectConstants.filePathArgValue,inputPath)
    val readStreamDF=readStreamFunction(spark,inputMap)

    readStreamDF.writeStream.format("delta").outputMode("append").option("checkpointLocation",checkpointLocation).foreachBatch(
      (microBatchDF:org.apache.spark.sql.DataFrame,batchID:Long) =>
        {
          finalTableManipulator(microBatchDF,batchID,persistPath,inputPath)
        }
    ).start

 /*

  spark-submit --class org.controller.samePointsAndDifferentTime.deltaStreamToUpdateTheFinalTable --driver-cores 2 --driver-memory 2g --executor-memory 1g --executor-cores 2 --num-executors 2 --conf spark.dynamic.memory.allocation.enabled=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0  /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tmp2/ propFileLocation=hdfs://localhost:8020/user/raptor/hadoop/keys/keysForCLI.txt topic=tmpTopic offset=earliest broker="raptor-VirtualBox:9194,raptor-VirtualBox:9195" kafkaTrustStoreType=jks keyDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" valueDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" inputPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmp2/"  persistPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmpFinal/"  clearCheckpointFlag=y kafkaSecurityProtocol=SSL

 val tmpColNames=Seq("driverName","points","receivedTimeStamp","lastUpdatedTimeStamp")
    val tmpSelectExprs=Seq("cast(driverName as string) driverName","cast(points as int) points","cast (receivedTimeStamp as timestamp) receivedTimeStamp","cast (lastUpdatedTimeStamp as timestamp) lastUpdatedTimeStamp")
    Seq("Vettel,20,2020-01-01 05:06:07.888,2020-01-01 05:06:07.888","Max,30,2020-01-02 05:06:07.888,2020-01-02 05:06:07.888").map(_.split(",")).map(x => (x(0).toString,x(1).toString,x(2).toString,x(3).toString)).toDF(tmpColNames:_*).selectExpr(tmpSelectExprs:_*).write.format("delta").save("hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmpFinal")*/
  spark.streams.awaitAnyTermination
  }

  def finalTableManipulator(microBatchDF:org.apache.spark.sql.DataFrame,batchID:Long,persistPath:String,inputPath:String)={
    val columnTmpSeq="driverName|batch.points as batch_points|batch.receivedTimeStamp as batch_receivedTimeStamp|base.points as base_points|base.receivedTimeStamp as base_receivedTimeStamp|base.lastUpdatedTimeStamp as base_lastUpdatedTimeStamp|updateFlag".split("\\|").toSeq
    import spark.implicits._
    val deltaTableFinal=DeltaTable.forPath(spark,persistPath)
    val deltaTableFinalDF=spark.read.format(projectConstants.deltaFormat).load(persistPath)
    val deltaTableBatchNameAndPoints=DeltaTable.forPath(spark,inputPath).toDF.groupBy("driverName").agg(sum("points").as("points")) // computing total points. This can be filtered by year so only 20 to 30 race entries or penalties might come
    val deltaTableBatchLastInTime=spark.read.format(projectConstants.deltaFormat).load(inputPath).groupBy("driverName").agg(max("receivedTimeStamp").as("receivedTimeStamp"))// taking the latest incoming timestamp
    val deltaTableBatch=deltaTableBatchNameAndPoints.join(deltaTableBatchLastInTime,Seq("driverName"))
    val dataNeedOfUpdation=deltaTableBatch.as("batch").join(deltaTableFinalDF.as("base"),Seq("driverName"),"left").where("base.receivedTimeStamp is not null").withColumn("updateFlag",when($"base.points"=== $"batch.points",lit("N")).otherwise(lit("Y"))).selectExpr(columnTmpSeq:_*).withColumn("lastUpdatedTimeStamp",lit(current_timestamp))
    val dataNeedOfInsertion=deltaTableBatch.as("batch").join(deltaTableFinalDF.as("base"),Seq("driverName"),"left").where("base.receivedTimeStamp is null").withColumn("updateFlag",lit("Y")).selectExpr(columnTmpSeq:_*).withColumn("lastUpdatedTimeStamp",lit(current_timestamp))
    val dataForUpsertion=dataNeedOfUpdation.union(dataNeedOfInsertion)
    // points update
    deltaTableFinal.as("base").merge(dataForUpsertion.filter($"updateFlag"==="Y").as("batch"),"batch.driverName = base.driverName").whenMatched.updateExpr(Map("points" -> "batch.batch_points","receivedTimeStamp" -> "batch.batch_receivedTimeStamp","lastUpdatedTimeStamp" ->s"batch.lastUpdatedTimeStamp" )).whenNotMatched.insertExpr(Map("driverName" -> "batch.driverName","points" -> "batch.batch_points","receivedTimeStamp" -> "batch.batch_receivedTimeStamp","lastUpdatedTimeStamp" ->"batch.lastUpdatedTimeStamp"/* this is from current timestamp added before s"${dateFormat.format(new Date)}" date is not changing correctly */)).execute
    // no points update
    deltaTableFinal.as("base").merge(dataForUpsertion.filter($"updateFlag"==="N").as("batch"),"batch.driverName = base.driverName").whenMatched.updateExpr(Map("lastUpdatedTimeStamp" ->s"batch.lastUpdatedTimeStamp")).execute // received timestamp must be considered while doing window, if points are same then update timestamp changes
    /*.whenMatched($"batch.points"=== $"base.points").updateExpr()*/
  }

}
