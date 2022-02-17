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
    spark-submit --master local --class org.controller.markCalculation.kafkaToAssessmentLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 readStreamFormat=kafka kafkaStartingOffsets=latest checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/" assessmentType=CA
SA
    spark-submit --master local --class org.controller.markCalculation.kafkaToAssessmentLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 readStreamFormat=kafka kafkaStartingOffsets=latest checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/" assessmentType=SA


*/
    innerMsgParser(getReadStreamDF(spark,inputMap).select(col("value").cast(StringType)).select(from_json(col("value"),wrapperSchema).as("schemaExploded")).select(col("schemaExploded.*")).filter(s"messageType='${inputMap(assessmentType)}'")).writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) =>
        //  foreachBatchFun(df,batchID,inputMap,cumulativeAssessment,spark)
        foreachBatchFunAssessment(spark,df,inputMap))
      .start.awaitTermination
 //   spark.streams.awaitAnyTermination
  }



  def foreachBatchFunAssessment(spark:org.apache.spark.sql.SparkSession,df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])=  Try{DeltaTable.forPath(spark,map("assessmentPath"))} match {
    case Success(s) => s.as("alfa").merge(df.as("delta"),col("alfa.studentID")===col("delta.StudentID")
      && col("alfa.examId")===col("delta.examId")
      && col("alfa.subjectCode")===col("delta.subjectCode")
     /* && col("alfa.revisionNumber")===col("delta.revisionNumber")*/)
      .whenNotMatched.insertAll
      .whenMatched(col("alfa.revisionNumber") === col("delta.revisionNumber")
        && col("delta.incomingTs" ) > col("alfa.incomingTs"))
      .updateExpr(Map("marks"->"delta.marks","incomingTs"->"delta.incomingTs"))
      .whenMatched(col("alfa.revisionNumber") =!= col("delta.revisionNumber") )
      .updateExpr(Map("incomingTs"->"alfa.incomingTs"))
      .execute

    case Failure(f) =>
      //  DeltaTable.create // cant create delta table through api, scala 2.12 is required
      map.put("writePath",map("assessmentPath"))
      map.put("writeFormat","delta")
      map.put("writeMode","append")
      persistDF(df,map)
   //   df.write.mode("append").format("delta").save(map("assessmentPath"))
  }

}
