package org.controller.markCalculation

import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import scala.util.{Failure, Success, Try}
object kafkaToAssessmentLoad {

  val spark= getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit={
    val inputMap=inputArrayToMap(args)
    // assessmentPath
    // assessmentType

    /*

CA
    spark-submit --master local --class org.controller.markCalculation.kafkaToAssessmentLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 readStreamFormat=kafka kafkaStartingOffsets=latest checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/" assessmentType=CA triggerPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_BronzeToTriggerInput/"
SA
    spark-submit --master local --class org.controller.markCalculation.kafkaToAssessmentLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 readStreamFormat=kafka kafkaStartingOffsets=latest checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/" assessmentType=SA triggerPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_BronzeToTriggerInput/"
// only updates if incomingTS is greater than what already is present in the table

*/
    val writeStreamDF=innerMsgParser(getReadStreamDF(spark,inputMap).select(col("value").cast(StringType)).select(from_json(col("value"),wrapperSchema).as("schemaExploded")).select(col("schemaExploded.*")).filter(s"messageType='${inputMap(assessmentType)}'"))


    writeStreamDF.writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) =>
        //  foreachBatchFun(df,batchID,inputMap,cumulativeAssessment,spark)
        foreachBatchFunAssessment(spark,df,inputMap))
      .start
/*  // batches may race, trigger might append first before original data gets updated
    writeStreamDF.writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation",inputMap("checkpointLocationTrigger"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) =>
        //  foreachBatchFun(df,batchID,inputMap,cumulativeAssessment,spark)
        foreachBatchFunAssessment(spark,df,inputMap))
      .start
   */

   spark.streams.awaitAnyTermination
  }

  def foreachBatchFunAssessment(df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])=  saveDF(df.write.mode("append").format("delta"),map("triggerPath"))



  def foreachBatchFunAssessment(spark:org.apache.spark.sql.SparkSession,df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])=  Try{DeltaTable.forPath(spark,map("assessmentPath"))} match {
    case Success(s) =>

      s.as("alfa").merge(df.as("delta"),col("alfa.studentID")===col("delta.studentID")
      && col("alfa.examId")===col("delta.examId")
      && col("alfa.subjectCode")===col("delta.subjectCode")
        && col("alfa.revisionNumber") === col("delta.revisionNumber")
      )
      .whenNotMatched.insertAll
      .whenMatched(col("delta.incomingTs" ) > col("alfa.incomingTs"))
        .updateExpr(Map("marks"->"delta.marks","incomingTs"->"delta.incomingTs"))
        .execute

      // input for trigger
      saveDF(df.write.mode("append").format("delta"),map("triggerPath"))

    case Failure(f) =>
      //  DeltaTable.create // cant create delta table through api, scala 2.12 is required
      map.put("writePath",map("assessmentPath"))
      map.put("writeFormat","delta")
      map.put("writeMode","append")
      persistDF(df,map)
   //   df.write.mode("append").format("delta").save(map("assessmentPath"))
      saveDF(df.write.mode("append").format("delta"),map("triggerPath"))
  }

}
