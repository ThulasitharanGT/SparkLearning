package org.controller.markCalculation
// sem level

import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
/*

 spark-submit --num-executors 2 --executor-cores 2 --driver-memory 512m --executor-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --class org.controller.markCalculation.diamondCalculation /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/" goldCAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Gold/" goldSAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Gold/" semIdExamIDMapping="hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDMapping/" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAGoldToDiamondCalc" examIdToExamType="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/"

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

    spark.udf.register("maxMarksUDF",getMaxMarksFinal(_:String))
    val maxMarksUDF=udf(getMaxMarksFinal(_:String):Int)
    val getGradeUDF=udf(getGradeJava(_:java.math.BigDecimal,_:java.math.BigDecimal,_:String):String)

    val semIdAndExamIdDF=spark.read.format("delta").load(inputMap("semIdExamIDMapping"))

    val examIdToExamTypeDF= spark.read.format("delta").load(inputMap("examIdToExamType"))

    examIdToExamTypeDF.createTempView("exam_type_table")
    semIdAndExamIdDF.createTempView("sem_table")

    spark.sql(s"""
              select examType,semId,examId,sum(rnk_col) over (partition by semId,examType ) num_of_assessments from
              (select a.examType,b.semId,a.examId,
                  row_number() over (partition by b.semId,a.examType,a.examId order by 1) as rnk_col
              from exam_type_table a join sem_table b on a.examId=b.examId
               order by a.examType,b.semId,a.examId) a where rnk_col=1 """).as("num_exams")
      .join(spark.table("exam_type_table").as("exam_info").join(spark.table("sem_table").as("sem_info"),Seq("examId"))
      .as("exam_info"),Seq("semId","examId","examType"))
      .createOrReplaceTempView("exam_id_sem_id_exam_type")

    val examIDSemIDAndTypeMappedDF= spark.table("exam_id_sem_id_exam_type")

    val semIdDF=  examIDSemIDAndTypeMappedDF
      .as("semIds")
      .join(df.as("trigger"),
        col("trigger.examId")===col("semIds.examId"))
       .select(col("trigger.examId"),col("semIds.semId"),
        col("trigger.studentId"),col("semIds.subjectCode")
       ,$"semIds.examType",col("num_of_assessments")).withColumn("rankCol",
      row_number.over(  org.apache.spark.sql.expressions.Window
        .partitionBy("semId","examId","studentId"
          ,"subjectCode","examType")
        .orderBy(col("semId"))
      ))
      .where($"rankCol"===lit(1))
      .drop("rankCol")
      .orderBy("semId,examId,examType,subjectCode,studentId".split(",").toSeq.map(col):_*)

    semIdDF.withColumn("semIdDF",lit("semIdDF")).show(false)

    val examIDsOfSemIdDF=examIDSemIDAndTypeMappedDF.as("semTarget")
      .join(semIdDF.as("semSource"),Seq("semId"))
      .select("semTarget.examId","semId","semSource.studentId"
        ,"semTarget.subjectCode"
        ,"semTarget.examType","semTarget.num_of_assessments") // cross joins exam type
      .withColumn("rankCol",
        row_number.over(  org.apache.spark.sql.expressions.Window
          .partitionBy("semId","examId","studentId"
            ,"subjectCode","examType")
           .orderBy(col("semId"))
        ))
      .where($"rankCol"===lit(1))
      .drop("rankCol")
      .orderBy("semId,examId,examType,subjectCode,studentId".split(",").toSeq.map(col):_*)

     examIDsOfSemIdDF.withColumn("examIDsOfSemIdDF",lit("examIDsOfSemIdDF")).show(false)

    val caExamIDsOfSemIdDF=examIDsOfSemIdDF.filter(col("examType")=== lit(summativeAssessment))
    val saExamIDsOfSemIdDF=examIDsOfSemIdDF.filter(s"examType ='${summativeAssessment}'")


    // logic 1  read separately , join method

    val saExamDetails=examIDsOfSemIdDF.filter(col("examType")=== lit(summativeAssessment))

    val caExamDetails=examIDsOfSemIdDF.where(col("examType")=== lit(cumulativeAssessment))

  // examIDsOfSemIdDF.where(col("examType")=== lit(cumulativeAssessment)).filter("examId in ('e001')")
     // add examID partition to SA and CA in future

    val saExamIdAndStudentIdInfo=saExamDetails
      .map(x=> Row(x.getAs[String]("examId"),
        x.getAs[String]("studentId")))(RowEncoder(new StructType(Array(
        StructField("examId",StringType,true)
        ,StructField("studentId",StringType,true)
      )
    ))).collect.toSeq.map(x=> (x.getAs[String]("examId"),
      x.getAs[String]("studentId"))).distinct

    val caExamIdAndStudentIdInfo=caExamDetails.select(
      "examId,studentId".split(",").toSeq.map(col):_*
    ).distinct.collect.toSeq.map(x=> (x.getAs[String]("examId"),
      x.getAs[String]("studentId")))

    val caGoldInfo=spark.read.format("delta").load(inputMap("goldCAPath"))

    val saGoldInfo=spark.read.format("delta").load(inputMap("goldSAPath"))

    val saRecordsForIncomingKeysDF=saGoldInfo
      .where(s"examId in ${getWhereCondition(
        saExamIdAndStudentIdInfo.map(_._1).toArray)} and studentId in ${getWhereCondition(
        saExamIdAndStudentIdInfo.map(_._2).toArray)}").withColumn("examType",lit(summativeAssessment))
      .as("readFromGold")
/*      .join(saGoldInfo
        .where(s"examId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._1).toArray)} and studentId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._2).toArray)}").groupBy("studentId").agg(countDistinct("examId").as("examsAttended"))
        .as("readFromGoldInner") , col("readFromGoldInner.studentId") === col("readFromGold.studentId")
      ).select("readFromGold.*","examsAttended")*/

    saRecordsForIncomingKeysDF.withColumn("saRecordsForIncomingKeysDF",lit("saRecordsForIncomingKeysDF")).show(false)

    val caRecordsForIncomingKeysDF=caGoldInfo
      .where(s"examId in ${getWhereCondition(
        caExamIdAndStudentIdInfo.map(_._1).toArray)} and studentId in ${getWhereCondition(
        caExamIdAndStudentIdInfo.map(_._2).toArray)}").withColumn("examType",lit(cumulativeAssessment))
      .as("readFromGold")
     /* .join(caGoldInfo
        .where(s"examId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._1).toArray)} and studentId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._2).toArray)}").groupBy("studentId").agg(countDistinct("examId").as("examsAttended"))
        .as("readFromGoldInner") , col("readFromGoldInner.studentId") === col("readFromGold.studentId")
        ).select("readFromGold.*","examsAttended")
*/


    caRecordsForIncomingKeysDF.withColumn("caRecordsForIncomingKeysDF",lit("caRecordsForIncomingKeysDF")).show(false)
    // atleast 1 ca must be attended, doesn't matter fail or pass.

    ///// else just read gold SA and CA and join with incoming studId
    // and ExamId and filter subTotal and calc. But this gives inconsistent results.


    val caMarksCalculated= caRecordsForIncomingKeysDF.join(caExamIDsOfSemIdDF
      ,Seq("examId","studentId","subjectCode","examType"),"right")
      .withColumn("maxMarksOfAssessmentCalculated",lit(40.0)/col("num_of_assessments"))
      .withColumn("percentageOfMarksObtained",round((col("marks")*(lit(100.0)/col("maxMarks"))),3))
      .withColumn("marksObtainedAsPerNewMaxMarks",round(col("percentageOfMarksObtained")* (col("maxMarksOfAssessmentCalculated")/lit(100.0)),3))
      .na.fill("NA").na.fill(0.0).withColumn("numAssessmentsAttended",
      when(col("grade") === lit("NA") &&
        col("result") === lit("NA"), "-1" ).otherwise(lit(0)))
      .withColumn("marksObtained",sum("marksObtainedAsPerNewMaxMarks").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","studentId","subjectCode")
      ))
      .withColumn("numAssessmentsAttended",max("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","examId","studentId","subjectCode")
      ))
      .withColumn("numAssessmentsAttended",sum("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","studentId","subjectCode")
      ))
      .withColumn("numAssessmentsAttended",(col("numAssessmentsAttended") + col("num_of_assessments") )
      .cast(IntegerType))
      .withColumn("rankCol", dense_rank.over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","studentId","subjectCode")
          .orderBy(asc("examId"),asc("assessmentYear"))
      )).where(col("rankCol") === lit(1))
      .select("semId|studentId|subjectCode|examType|marksObtained|num_of_assessments|numAssessmentsAttended".split("\\|").map(col).toSeq:_*)
      .orderBy("semId","studentId","subjectCode")
/*

      .groupBy("semId","examType","studentId","subjectCode","num_of_assessments")
      .agg(sum("marksObtainedAsPerNewMaxMarks").as("marksObtained"),
       max("numAssessmentsAttended" ).as("numAssessmentsAttended"))
      .withColumn("numAssessmentsAttended",col("numAssessmentsAttended") + col("num_of_assessments"))
      .na.fill("NA").na.fill(0.0).orderBy("semId","studentId","subjectCode")
*/

    val saMarksCalculated= saRecordsForIncomingKeysDF.join(saExamIDsOfSemIdDF
      ,Seq("examId","studentId","subjectCode","examType"),"right")
      .withColumn("maxMarksOfAssessmentCalculated",lit(40.0)/col("num_of_assessments"))
      .withColumn("percentageOfMarksObtained",round((col("marks")*(lit(100.0)/col("maxMarks"))),3))
      .withColumn("marksObtainedAsPerNewMaxMarks",round(col("percentageOfMarksObtained")* (col("maxMarksOfAssessmentCalculated")/lit(100.0)),3))
      .na.fill("NA").na.fill(0.0).withColumn("numAssessmentsAttended",
      when(col("grade") === lit("NA") &&
        col("result") === lit("NA"), "-1" ).otherwise(lit(0)))
      .withColumn("marksObtained",sum("marksObtainedAsPerNewMaxMarks").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","studentId","subjectCode")
      ))
      .withColumn("numAssessmentsAttended",max("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","examId","studentId","subjectCode")
      ))
      .withColumn("numAssessmentsAttended",sum("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","studentId","subjectCode")
      ))
      .withColumn("numAssessmentsAttended",(col("numAssessmentsAttended") + col("num_of_assessments") )
        .cast(IntegerType))
      .withColumn("rankCol", dense_rank.over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId","examType","studentId","subjectCode")
          .orderBy(asc("examId"),asc("assessmentYear"))
      )).where(col("rankCol") === lit(1))
      .select("semId|studentId|subjectCode|examType|marksObtained|num_of_assessments|numAssessmentsAttended".split("\\|").map(col).toSeq:_*)
      .orderBy("semId","studentId","subjectCode")


   val resultDF= saMarksCalculated.as("sa")
      .join(caMarksCalculated.as("ca"),
        Seq("subjectCode","semId","studentId"))
    .withColumn("passMarkForSA",round(lit(50.0)*(lit(60.0)/lit(100.0)),3))
      .withColumn("passMarkForSem",round(lit(60.0),3))
      .withColumn("finalMarks",
        round(col("sa.marksObtained")+col("ca.marksObtained"),3))
      .withColumn("result",when(
        col("sa.marksObtained")>=col("passMarkForSA")
        && col("ca.numAssessmentsAttended") > lit(0)
        &&  col("finalMarks") >= col("passMarkForSem")
        ,lit("PASS")).otherwise(lit("FAIL")))
      .withColumn("grade",getGradeUDF(lit(new java.math.BigDecimal(100.0)),col("finalMarks").cast(DecimalType(6,3))
        ,lit("finalCalculation")))
      .withColumn("remarks",when(col("result")===lit("PASS")
        ,lit("Keep pushing")).when(col("result")===lit("FAIL") &&
        (col("sa.marksObtained") > col("passMarkForSA") ||
          col("sa.marksObtained") === col("passMarkForSA") ) &&
        col("finalMarks") < col("passMarkForSem"),
        lit("Cleared SA, but inadequate marks in CA"))
        .when(col("result")===lit("FAIL") &&
          (col("sa.numAssessmentsAttended") === lit(0) ||
            col("ca.numAssessmentsAttended") === lit(0) ) ,
          lit("Not appeared in adequate exams, Kindly maintain cognizance towards assessments"))
        .when(col("result")===lit("FAIL") &&
          col("sa.marksObtained") > col("passMarkForSA") &&
          col("finalMarks") > col("passMarkForSem")
          &&
          col("sa.numAssessmentsAttended") == lit(0),
          lit("Cleared SA, but have not even attended CA")
       ).when(col("result")===lit("FAIL") &&
        col("sa.marksObtained") < col("passMarkForSA")  ,
        lit("Failed in SA")).otherwise("NA"))
      .select("subjectCode,semId,studentId,passMarkForSA,passMarkForSem,finalMarks,remarks,grade,result"
        .split(",").map(col).toSeq ++
        Seq(col("sa.marksObtained").as("SA_marks")
          ,col("ca.marksObtained").alias("CA_marks"),
          col("ca.numAssessmentsAttended").as("CA_appeared"),
          col("sa.numAssessmentsAttended").as("SA_appeared"),
          col("ca.num_of_assessments").as("totalAssessmentsInCA"),
          col("sa.num_of_assessments").as("totalAssessmentsInSA")) :_*)

    resultDF.show(false)

  }



}
