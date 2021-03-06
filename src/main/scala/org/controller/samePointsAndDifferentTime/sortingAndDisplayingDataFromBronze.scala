package org.controller.samePointsAndDifferentTime

import org.apache.spark.sql.SparkSession
import org.constants.projectConstants
import org.util.SparkOpener
import org.util.readWriteUtil.{checkPointLocationCleaner, readDF, readStreamFunction, writeDF}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object sortingAndDisplayingDataFromBronze extends SparkOpener{
// OUTDATED
  def main(args:Array[String]):Unit ={
    val spark=SparkSessionLoc()
    val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args)
      inputMap.put(arg.split("=")(0),arg.split("=")(1))
    val checkpointLocation=inputMap("checkpointLocation")
    val inputPath=inputMap("inputPath")
    val persistPath=inputMap("persistPath")
    val clearCheckpointFlag = inputMap("clearCheckpointFlag")
    println(s"Checking if checkpoint exists in ${checkpointLocation}")
    checkPointLocationCleaner(checkpointLocation,clearCheckpointFlag)

    inputMap.put(projectConstants.fileFormatArg,projectConstants.fileTypeDeltaValue)
    inputMap.put(projectConstants.filePathArgValue,inputPath)

    readStreamFunction(spark,inputMap).writeStream.format("console").outputMode("update").option("checkpointLocation",checkpointLocation).foreachBatch(
      (microBatchDF:org.apache.spark.sql.DataFrame,batchID:Long) => {
        reComputeDriversPositions(spark,inputPath,persistPath,inputMap)
      }
    ).start


    spark.streams.awaitAnyTermination
  }
  def reComputeDriversPositions(spark:SparkSession,inputPath:String,persistPath:String,inputMap:collection.mutable.Map[String,String]) ={
    inputMap.put(projectConstants.fileTypeArgConstant, projectConstants.fileTypeDeltaValue)
    inputMap.put(projectConstants.filePathArgValue, inputPath)
    val driverPositionUpdatesDF=readDF(spark,inputMap)  //spark.read.format(projectConstants.deltaFormat).load(inputPath)
    inputMap.put(projectConstants.filePathArgValue, persistPath)
    inputMap.put(projectConstants.fileOverwriteAppendArg, projectConstants.fileOverwriteValue)
    writeDF(inputMap,driverPositionUpdatesDF.withColumn("driverStandings",rank over(Window.orderBy(desc("points"),asc("receivedTimeStamp")))))
  }

  /*

  	at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:245)
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Detected a data update in the source table. This is currently not supported. If you'd like to ignore updates, set the option 'ignoreChanges' to 'true'. If you would like the data update to be reflected, please restart this query with a fresh checkpoint directory.
=== Streaming Query ===


You cant update the main table, ie the tmpFinalTable. run a batch code after evey update like a 1 hour once job for the update. make that as a delta code

    spark-submit --class org.controller.samePointsAndDifferentTime.sortingAndDisplayingDataFromBronze --driver-cores 2 --driver-memory 2g --executor-memory 1g --executor-cores 2 --num-executors 2 --conf spark.dynamic.memory.allocation.enabled=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0  /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tmp3/ inputPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmpFinal/"  persistPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmpFinalAgg/"  clearCheckpointFlag=y
*/

}
