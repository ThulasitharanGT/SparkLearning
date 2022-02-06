package org.controller.markCalculation

import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import io.delta.tables._
import scala.util.{Try,Success,Failure}

object kafkaToBronzeLoad {
  val spark=getSparkSession()

  def main(args:Array[String]):Unit = {
   val inputMap= inputArrayToMap(args)

    /*  kafkaSubscribeAssignDecider=kafkaSubscribe
        kafkaSubscribe=topicTmp
        kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083
        SAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/"
        CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/"
        bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/"
        bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/"
        SACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/"
        CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/"

spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,io.delta:delta-core_2.12:1.1.0
     */


    val readStreamDF=getReadStreamDF(spark,inputMap).select(col("value").cast(StringType)).select(from_json(col("value"),wrapperSchema).as("schemaExploded")).select(col("schemaExploded.*"))

    val summativeAssessmentStream=readStreamDF.filter(s"messageType='${summativeAssessment}'")

    val cumulativeAssessmentStream=readStreamDF.filter(s"messageType='${cumulativeAssessment}'")

    readStreamDF.writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("bronzeCheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap,"bronze") )
      .start

    innerMsgParser(cumulativeAssessmentStream).writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("CACheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap,cumulativeAssessment) )
      .start

    innerMsgParser(summativeAssessmentStream).writeStream.format("console").outputMode("append").option("checkpointLocation",inputMap("CACheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap,summativeAssessment) )
      .start


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
