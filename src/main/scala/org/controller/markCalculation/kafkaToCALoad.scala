package org.controller.markCalculation
// DEPRECIATED
import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._

import scala.util.{Failure, Success, Try}


object kafkaToCALoad {
  val spark=getSparkSession()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")

  def foreachBatchFunCA(spark:org.apache.spark.sql.SparkSession,df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])=  Try{DeltaTable.forPath(spark,map("CAPath"))} match {
    case Success(s) => s.as("alfa").merge(df.as("delta"),col("alfa.studentID")===col("delta.StudentID")
      && col("alfa.examId")===col("delta.examId")
      && col("alfa.subjectCode")===col("delta.subjectCode")
      && col("alfa.revisionNumber")===col("delta.revisionNumber")).whenNotMatched.insertAll
      .whenMatched.updateExpr(Map("alfa.marks"->"delta.marks","alfa.incomingTs"->"delta.incomingTs")).execute
    case Failure(f) =>
      //  DeltaTable.create // cant create delta table through api, scala 2.12 is required in gradle build
      df.write.mode("append").format("delta").save(map("CAPath"))
  }

  def main(args:Array[String]):Unit={
    /*
       spark-submit --master local --class org.controller.markCalculation.kafkaToCALoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 readStreamFormat=kafka kafkaStartingOffsets=latest CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/" CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/"

    */
    val inputMap=inputArrayToMap(args)
   // val readStreamDF=getReadStreamDF(spark,inputMap)


    innerMsgParser(getReadStreamDF(spark,inputMap).select(col("value").cast(StringType)).select(from_json(col("value"),wrapperSchema).as("schemaExploded")).select(col("schemaExploded.*")).filter(s"messageType='${cumulativeAssessment}'")).writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation",inputMap("CACheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) =>
        foreachBatchFunCA(spark,df,inputMap))
      //  foreachBatchFun(df,batchID,inputMap,summativeAssessment,spark) )
      .start

    spark.streams.awaitAnyTermination
  }

}
