package org.controller.markCalculation

import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import io.delta.tables._

import scala.util.{Failure, Success, Try}

object kafkaToBronzeLoad {
  val spark=getSparkSession()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")

  def main(args:Array[String]):Unit = {
   val inputMap= inputArrayToMap(args)
    inputMap foreach println
    /*  kafkaSubscribeAssignDecider=kafkaSubscribe
        kafkaSubscribe=topicTmp
        kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083
        SAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/"
        CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/"
        bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/"
        bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/"
        SACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/"
        CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/"
        consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"
kafkaStartingOffsetsArg=latest
--master yarn --deploy-mode cluster
readStreamFormat=kafka

spark-submit --master local --class org.controller.markCalculation.kafkaToBronzeLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,io.delta:delta-core_2.12:1.1.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 SAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/" CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/" bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/" bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/" SACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/" CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/" readStreamFormat=kafka kafkaStartingOffsets=latest consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"
     */

    val readStreamDF=getReadStreamDF(spark,inputMap).select(col("value").cast(StringType)).select(from_json(col("value"),wrapperSchema).as("schemaExploded")).select(col("schemaExploded.*"))
    println(s"readStreamDF after")

    val summativeAssessmentStream=readStreamDF.filter(s"messageType='${summativeAssessment}'")
    println(s"summativeAssessmentStream after")

    val cumulativeAssessmentStream=readStreamDF.filter(s"messageType='${cumulativeAssessment}'")
    println(s"cumulativeAssessmentStream after")

    readStreamDF.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("bronzeCheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap,"bronze") )//.trigger(Trigger.Continuous(1000))
      .start

    innerMsgParser(cumulativeAssessmentStream).writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("CACheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap,cumulativeAssessment) )//.trigger(Trigger.Continuous(1000))
      .start

    innerMsgParser(summativeAssessmentStream).writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("CACheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap,summativeAssessment) )//.trigger(Trigger.Continuous(1000))
      .start

    readStreamDF.writeStream.format("console").outputMode("append").option("truncate","false").option("numRows","99999999").option("checkpointLocation",inputMap("consoleCheckpoint")).start

    println(s"before await any termination")

    spark.streams.awaitAnyTermination


  }

  def innerMsgParser(df:org.apache.spark.sql.DataFrame)=df.select(from_json(col("actualMessage"),innerMarksSchema).as("structExtracted"),udfTSFromString(col("receivingTimeStamp")).as("incomingTS")).select(col("structExtracted.*"),col("incomingTS"))


  val simpleDateFormat= new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  def getTSFromString(tsStr:String)=new java.sql.Timestamp(simpleDateFormat.parse(tsStr).getTime)

  val udfTSFromString= udf(getTSFromString(_:String))

  val foreachBatchFun:(org.apache.spark.sql.DataFrame,Long,collection.mutable.Map[String,String],String) =>Unit=
    (df:org.apache.spark.sql.DataFrame,batchID:Long,tmpMap:collection.mutable.Map[String,String],controlString:String)=>controlString match {
      case value if value == "bronze" =>
        tmpMap.put(writeStreamFormat,deltaStreamFormat)
        tmpMap.put(deltaMergeOverwriteDecider,deltaMerge)
        tmpMap.put(deltaMerge,"false")
        tmpMap.put(pathArg,tmpMap("bronzePath"))
        foreachBatchFunBronze(df.withColumn("batchID",lit(batchID)),tmpMap)
      case value if value == summativeAssessment =>
        tmpMap.put(writeStreamFormat,deltaStreamFormat)
        tmpMap.put(deltaMergeOverwriteDecider,deltaMerge)
        tmpMap.put(deltaMerge,"false")
        tmpMap.put(pathArg,tmpMap("SAPath"))
        foreachBatchFunSA(df,tmpMap)
      case value if value == cumulativeAssessment =>
        tmpMap.put(writeStreamFormat,deltaStreamFormat)
        tmpMap.put(deltaMergeOverwriteDecider,deltaMerge)
        tmpMap.put(deltaMerge,"false")
        tmpMap.put(pathArg,tmpMap("CAPath"))
        foreachBatchFunCA(df,tmpMap)
    }


  def foreachBatchFunBronze(df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])= dfWriterStream(spark,df,map)

  def foreachBatchFunSA(df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])=  Try{DeltaTable.forPath(spark,map("SAPath"))} match {
    case Success(s) => s.as("alfa").merge(df.as("delta"),col("alfa.studentID")===col("delta.StudentID")
      && col("alfa.examId")===col("delta.examId")
      && col("alfa.subjectCode")===col("delta.subjectCode")
      && col("alfa.revisionNumber")===col("delta.revisionNumber")).whenNotMatched.insertAll
      .whenMatched.updateExpr(Map("alfa.marks"->"delta.marks","alfa.incomingTs"->"delta.incomingTs")).execute
    case Failure(f) =>
    //  DeltaTable.create // cant create delta table through api, scala 2.12 is required
      df.write.mode("append").format("delta").save(map("SAPath"))
  }

  def foreachBatchFunCA(df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])=  Try{DeltaTable.forPath(spark,map("CAPath"))} match {
    case Success(s) => s.as("alfa").merge(df.as("delta"),col("alfa.studentID")===col("delta.StudentID")
      && col("alfa.examId")===col("delta.examId")
      && col("alfa.subjectCode")===col("delta.subjectCode")
      && col("alfa.revisionNumber")===col("delta.revisionNumber")).whenNotMatched.insertAll
      .whenMatched.updateExpr(Map("alfa.marks"->"delta.marks","alfa.incomingTs"->"delta.incomingTs")).execute
    case Failure(f) =>
      //  DeltaTable.create // cant create delta table through api, scala 2.12 is required
      df.write.mode("append").format("delta").save(map("CAPath"))
  }

}
