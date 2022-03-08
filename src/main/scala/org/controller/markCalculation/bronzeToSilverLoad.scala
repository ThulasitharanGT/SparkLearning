package org.controller.markCalculation
// will read the intermediate path and update silver

import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import scala.util.{Failure, Success, Try}


// use this as trigger "hdfs://localhost:8020/user/raptor/persist/marks/SA_BronzeToTriggerInput/" and read from "hdfs://localhost:8020/user/raptor/persist/marks/SA/" to take the latest record and update in silver.
/*

// takes latest revision and latest timestamp record, record for studentID,examId,subjectCode combination and

SA
spark-submit --class org.controller.markCalculation.bronzeToSilverLoad --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --driver-cores 2 --driver-memory 512m --executor-cores 2 --num-executors 2 --executor-memory 512m --master local /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/SA/" silverPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Silver/" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAInter" readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/SA_BronzeToTriggerInput/"  triggerPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_SilverToTriggerInput/"

CA
spark-submit --class org.controller.markCalculation.bronzeToSilverLoad --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --driver-cores 2 --driver-memory 512m --executor-cores 2 --num-executors 2 --executor-memory 512m --master local /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/CA/" silverPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Silver/" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInter" readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/CA_BronzeToTriggerInput/" triggerPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_SilverToTriggerInput/"


*/
object bronzeToSilverLoad {
  val spark=getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit ={
    val inputMap=inputArrayToMap(args)

    getReadStreamDF(spark,inputMap).writeStream.format("console")
      .outputMode("append").option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long) =>
        forEachBatchFun({
          val requiredStudentIdExamIDAndSubCode=df.select(col("studentID"),
            col("examId"), col("subjectCode")
          ).distinct //.dropDuplicates("studentID","examId","subjectCode")
            .collect.map(x=> (x.getAs[String]("studentID"),
            x.getAs[String]("examId"),x.getAs[String]("subjectCode")))

          spark.read.format("delta").load(inputMap("bronzePath"))
            .filter(s"studentId in ${getWhereCondition(requiredStudentIdExamIDAndSubCode.map(_._1))}")
            .where(s"examId in ${getWhereCondition(requiredStudentIdExamIDAndSubCode.map(_._2))} and subjectCode in ${getWhereCondition(requiredStudentIdExamIDAndSubCode.map(_._3))}")
            .withColumn("rankCol",row_number.over(org.apache.spark.sql.expressions.Window.partitionBy(col("studentID"),
              col("examId"), col("subjectCode")).orderBy(desc("revisionNumber"),desc("incomingTS"))))
            .filter("rankCol =1").drop("rankCol")
        },inputMap)).start

    spark.streams.awaitAnyTermination

   /*  .merge(.as("delta")
      ,col("alfa.studentID")===col("delta.studentID")
          && col("alfa.examId")===col("delta.examId")
          && col("alfa.subjectCode")===col("delta.subjectCode")
      ).whenMatched(col("alfa.marks") === col("delta.marks"))
      .updateExpr(Map("delta.incomingTs"->"alfa.incomingTs"))
      .whenMatched
      .updateExpr(Map("delta.incomingTs"->"incomingTs","delta.marks"->"alfa.marks"))
      .whenNotMatched
      .insertAll
      .execute
      */
  }

  val forEachBatchFun:(org.apache.spark.sql.DataFrame
    ,collection.mutable.Map[String,String])
    => Unit= (dfBatch:org.apache.spark.sql.DataFrame
              ,inputMap:collection.mutable.Map[String,String]) =>
    Try{getDeltaTable(spark,inputMap("silverPath"))} match {
    case Success(s)=> s.as("alfa").merge(dfBatch.as("delta")
  , col("alfa.studentID") === col("delta.studentID")
  && col("alfa.examId") === col("delta.examId")
  && col("alfa.subjectCode") === col("delta.subjectCode")
  ).whenMatched(col("alfa.marks") === col("delta.marks") && col("alfa.revisionNumber") === col("delta.revisionNumber")  )
  .updateExpr(Map("incomingTs" -> "alfa.incomingTs") )
  .whenMatched(col("alfa.revisionNumber") === col("delta.revisionNumber")  )
  .updateExpr(Map( "marks" -> "delta.marks","incomingTs" -> "delta.incomingTs"))
  .whenMatched(col("alfa.revisionNumber") =!= col("delta.revisionNumber") )
  .updateExpr(Map("incomingTs" -> "delta.incomingTs", "marks" -> "delta.marks", "revisionNumber" -> "delta.revisionNumber") )
  .whenNotMatched
  .insertAll
  .execute

      saveDF(dfBatch.write.mode("append").format("delta"),inputMap("triggerPath"))

    // write to gold trigger
    case Failure(f)=>
      dfBatch.write.mode("append").format("delta").save(inputMap("silverPath"))

      saveDF(dfBatch.write.mode("append").format("delta"),inputMap("triggerPath"))
      // write to gold trigger
    }



}
