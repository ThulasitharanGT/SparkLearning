package org.controller.markCalculation
// sem level

import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
/*

 spark-submit --num-executors 2 --executor-cores 2 --driver-memory 512m --executor-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --class org.controller.markCalculation.diamondCalculation /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/SA_GoldToTriggerInput/" goldCAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Gold/" goldSAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Gold/" semIdExamIDMapping="hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDMapping/" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAGoldToDiamondCalc" examIdToExamType="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/"

* */

object diamondCalculation {
  val spark=getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  // mark calculation
  // 70 % from SA
  // 30 % from CA
  // if you failed in SA (on any subject) then you failed
  // if you cleared SA but failed in CA and get 50 % , then you passed
def main(args:Array[String]):Unit ={

    val inputMap=inputArrayToMap(args)

    getReadStreamDF(spark,inputMap).writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch(
        (df:org.apache.spark.sql.DataFrame,batchID:Long)=>forEachBatchFunction(df, inputMap))
      .start

    spark.streams.awaitAnyTermination

// semID to examID mapping
    /*
    Seq("sem001,e001","sem001,e002","sem001,ex001")
    .map(_.split(",")).map(x=>(x(0),x(1)))
    .toDF("semId,examId".split(","):_*)
    .write.mode("overwrite").format("delta").save("hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDMapping")


    semIdExamIDMapping="hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDMapping/"


    */
  }

  def forEachBatchFunction(df:org.apache.spark.sql.DataFrame,inputMap:collection.mutable.Map[String,String])={
  /*  val keysToReadFromGold=df.map(x=> Row(x.getAs[String]("studentId")
    ,x.getAs[String]("examId")
    ,x.getAs[String]("assessmentYear")
    ,x.getAs[String]("examType")))(RowEncoder
    (new org.apache.spark.sql.types.StructType(
      Array(
        org.apache.spark.sql.types.StructField("studentId",StringType,true)
     ,org.apache.spark.sql.types.StructField("examId",StringType,true)
      ,org.apache.spark.sql.types.StructField("assessmentYear",StringType,true)
      ,org.apache.spark.sql.types.StructField("examType",StringType,true)))))
      .collect.toList.map(x=>(x.getAs[String]("studentId")
      ,x.getAs[String]("examId")
      ,x.getAs[String]("assessmentYear")
      ,x.getAs[String]("examType")))
*/
    val semIdAndExamIdDF=spark.read.format("delta").load(inputMap("semIdExamIDMapping"))

    val semIdDF=  semIdAndExamIdDF
      //.filter(s"examId in ${keysToReadFromGold.map(_._2)}")
      .as("semIds")
      .join(df.as("trigger"),
        col("trigger.examId")===col("semIds.examId"))
      .select(col("trigger.examId"),col("semIds.semId"),
        col("trigger.studentId"))

    semIdDF.withColumn("semIdDF",lit("semIdDF")).show(false)

    val examIDsOfSemIdDF=semIdAndExamIdDF.as("semTarget")
      .join(semIdDF.as("semSource"),Seq("semId"))
      .select("semTarget.examId","semId","studentId")
    //  .withColumn("rankCol",row_number.over(
      .withColumn("rankCol",
        row_number.over(  org.apache.spark.sql.expressions.Window
          .partitionBy("semId","examId","studentId")
        //.orderBy(lit(1))
        .orderBy(col("semId"))
      ))
     .where($"rankCol"===lit(1))
      .drop("rankCol")

    examIDsOfSemIdDF.show(false)

    val examIdToExamTypeDF=spark.read.format("delta").load(inputMap("examIdToExamType"))

    val examTypeAndExamIdMapped=examIDsOfSemIdDF.join(examIdToExamTypeDF,
      Seq("examId"))

    val saExamDetails=examTypeAndExamIdMapped.filter(col("examType")=== lit(summativeAssessment))

    val caExamDetails=examTypeAndExamIdMapped.filter(col("examType")=== lit(cumulativeAssessment))


    val caExamIdAndStudentIdInfo=saExamDetails
      .map(x=> Row(x.getAs[String]("examId"),
        x.getAs[String]("studentId"),
        x.getAs[String]("examType")))(RowEncoder(new StructType(Array()
    )))


    val caGoldInfo=spark.read.format("delta").load("")

    val saGoldInfo=spark.read.format("delta").load("")


///// else just read gold SA and CA and join with incoming studId
    // and ExamId and filter subTotal and calc. But this gives inconsistent results.




    // read semID for all examId.

    // for those examID and studentID read total and then calculate


    // examId|studentId|assessmentYear  |examType

  }



}
