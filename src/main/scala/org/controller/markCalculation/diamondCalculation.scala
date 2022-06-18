package org.controller.markCalculation
// sem level

import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.math.MathContext
import org.apache.spark.sql.expressions.Window
/*

 spark-submit --num-executors 2 --executor-cores 2 --driver-memory 512m --executor-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --class org.controller.markCalculation.diamondCalculation /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/" goldCAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Gold/" goldSAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Gold/" semIdExamIDAssessmentYear="hdfs://localhost:8020/user/raptor/persist/marks/assessmentYearInfo_scd2/" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAGoldToDiamondCalc" examIdToExamType="hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDAndExamType_scd2/" diamondPath="hdfs://localhost:8020/user/raptor/persist/marks/diamondCalculatedAndPartitioned/" semIdAndExamIdAndSubCode=hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDAndSubCode_scd2/

* */

object diamondCalculation {
  val spark = getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  spark.udf.register("maxMarksUDF", getMaxMarksFinal(_: String))

  val maxMarksUDF = udf(getMaxMarksFinal(_: String): Int)
  val getGradeUDF = udf(getGradeJava(_: java.math.BigDecimal, _: java.math.BigDecimal, _: String): String)
  val arrayFilterEqualsUDF = udf(array_filter_equals[String](_: Seq[String], _: String): Seq[String])
  val arrayFilterContainsUDF = udf(array_filter_contains[String](_: Seq[String], _: String): Seq[String])

  import spark.implicits._

  // mark calculation
  // 60 % from SA
  // 40 % from CA
  // if you failed in SA (on any subject) then you failed
  // if you cleared SA but failed in CA and get 60 % , then you passed
  def main(args: Array[String]): Unit = {

    val inputMap = inputArrayToMap(args)

    getReadStreamDF(spark, inputMap).writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation", inputMap("checkpointLocation"))
      .foreachBatch(
        (df: org.apache.spark.sql.DataFrame, batchID: Long) => forEachBatchFunction(df, inputMap))
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

  def forEachBatchFunction(df: org.apache.spark.sql.DataFrame, inputMap: collection.mutable.Map[String, String]) = {

     //  val df= spark.read.format("delta").load("hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/").filter(" (examId in ('ex001','e001') and studentId ='stu001')  or (studentId ='stu002')")
// assessmentYear
    val semIdExamIdAndAssessmentYearDF = spark.read.format("delta").load(inputMap("semIdExamIDAssessmentYear")).where("endDate is null").drop("endDate","startDate")
// examType
    val examIdToExamTypeDF = spark.read.format("delta").load(inputMap("examIdToExamType")).where("endDate is null").drop("endDate","startDate")
// examType
    val semIdAndExamIdToSubCodeDF = spark.read.format("delta").load(inputMap("semIdAndExamIdAndSubCode")).where("endDate is null").drop("endDate","startDate")



    examIdToExamTypeDF.createOrReplaceTempView("exam_type_table")
    semIdExamIdAndAssessmentYearDF.createOrReplaceTempView("assessment_year_table")
    semIdAndExamIdToSubCodeDF.createOrReplaceTempView("sem_exam_subject")


 /*   spark.sql(
      s"""
              select examType,semId,examId,max(rnk_col) over (partition by semId,examType) num_of_assessments from
              (select a.examType,b.semId,a.examId, dense_rank() over(partition by semId,examType order by a.examId )
             rnk_col from exam_type_table a join sem_table b on a.examId=b.examId
               order by a.examType,b.semId,a.examId) a """).as("num_exams")
      .join(spark.table("exam_type_table").as("exam_info").join(
        spark.table("sem_exam_subject").as("sem_info"), Seq("examId","semId")
      )
        .as("exam_info"), Seq("semId", "examId", "examType"))
      .createOrReplaceTempView("exam_id_sem_id_exam_type")*/

    /*
         spark.sql(s"""
              select examType,semId,examId,sum(rnk_col) over (partition by semId,examType ) num_of_assessments from
              (select a.examType,b.semId,a.examId,
                  row_number() over (partition by b.semId,a.examType,a.examId order by 1) as rnk_col
              from exam_type_table a join sem_table b on a.examId=b.examId
               order by a.examType,b.semId,a.examId) a where rnk_col=1 """).as("num_exams")
      .join(spark.table("exam_type_table").as("exam_info").join(spark.table("sem_table").as("sem_info"),Seq("examId"))
      * */

    /* select  a.examId,a.semId,b.assessmentYear,b.examType,a.subjectCode,b.num_of_assessments_per_type
     from sem_exam_subject a join (select examId,semId,assessmentYear,examType,
     count(examId) over (partition by semId,assessmentYear,examType ) as num_of_assessments_per_type
     from (select a.examId,a.semId,b.assessmentYear,a.examType,
     row_number() over(partition by a.examId,b.semId,b.assessmentYear,a.examType order by a.examType) rankCol
      from exam_type_table a join assessment_year_table b on a.semId=b.semId and a.examId=b.examId order by a.examId,a.semId,a.examType)
       a where a.rankCol=1)b on a.semId=b.semId and a.examId=b.examId
       // examId and semId is a primary key combo

    */

    spark.table("exam_type_table").as("a").join(
      spark.table("assessment_year_table").as("b"),"examId,semId".split(",").toSeq).
      withColumn("number_of_assessments_per_exam_type",
        count("examId").over(org.apache.spark.sql.expressions.Window
          .partitionBy("semId,b.assessmentYear,a.examType".split(",").map(col):_*))
      ).select("examId,semId,b.assessmentYear,a.examType,number_of_assessments_per_exam_type".split(",").map(col):_*)
      .as("b").join(spark.table("sem_exam_subject").as("b"),"examId,semId".split(",").toSeq)
      .orderBy("examId,semId,subjectCode".split(",").map(asc):_*).createOrReplaceTempView("exam_id_sem_id_exam_type")

    val examIDSemIDAndTypeMappedDF = spark.table("exam_id_sem_id_exam_type")

    examIDSemIDAndTypeMappedDF.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    spark.table("exam_id_sem_id_exam_type").withColumn("finalRef",lit("finalRef")).show(false)

    // takes semID of incoming examID

    val semIdDF = examIDSemIDAndTypeMappedDF
      .as("semIds")
      .join(df.as("trigger"),
        col("trigger.examId") === col("semIds.examId"))
      .select(col("trigger.examId"), col("semIds.semId"),
        col("trigger.studentId"), col("semIds.subjectCode")
        , $"semIds.examType", col("semIds.number_of_assessments_per_exam_type")
        , col("semIds.assessmentYear") ).withColumn("rankCol",
      row_number.over(org.apache.spark.sql.expressions.Window
        .partitionBy("semId", "examId","assessmentYear", "studentId"
          , "subjectCode", "examType")
        .orderBy(col("semId"))
      ))
      .where($"rankCol" === lit(1))
      .drop("rankCol")
      .orderBy("semId,examId,examType,subjectCode,studentId".split(",").toSeq.map(col): _*)

    semIdDF.withColumn("semIdDF", lit("semIdDF")).show(false)

    // takes examIds of incoming examID's semId

    val examIDsOfSemIdDF = examIDSemIDAndTypeMappedDF.as("semTarget")
      .join(semIdDF.as("semSource"), Seq("semId"))
      .selectExpr("semTarget.examId", "semId", "semSource.studentId"
        , "semTarget.subjectCode"
        , "semTarget.examType", "semTarget.number_of_assessments_per_exam_type as number_of_assessments") // cross joins exam type
      .withColumn("rankCol",
        row_number.over(org.apache.spark.sql.expressions.Window
          .partitionBy("semId", "examId", "studentId"
            , "subjectCode", "examType")
          .orderBy(col("semId"))
        ))
      .where($"rankCol" === lit(1))
      .drop("rankCol")
      .orderBy("semId,examId,examType,subjectCode,studentId".split(",").toSeq.map(col): _*)

    /*
   val examIDsOfSemIdDFTmp=examIDsOfSemIdDF.union(examIDsOfSemIdDF.withColumn("studentId",lit("s002")))
   val examIDsOfSemIdDF=examIDsOfSemIdDFTmp
    */

    examIDsOfSemIdDF.withColumn("examIDsOfSemIdDF", lit("examIDsOfSemIdDF")).show(false)

    val caExamIDsOfSemIdDF = examIDsOfSemIdDF.filter(col("examType") === lit(cumulativeAssessment))
    val saExamIDsOfSemIdDF = examIDsOfSemIdDF.filter(s"examType ='${summativeAssessment}'")

    // examIDsOfSemIdDF.where(col("examType")=== lit(cumulativeAssessment)).filter("examId in ('e001')")
    // add examID partition to SA and CA in future

    val saExamIdAndStudentIdInfo = saExamIDsOfSemIdDF
      .map(x => Row(x.getAs[String]("examId"),
        x.getAs[String]("studentId")))(RowEncoder(new StructType(Array(
        StructField("examId", StringType, true)
        , StructField("studentId", StringType, true)
      )
      ))).collect.toSeq.map(x => (x.getAs[String]("examId"),
      x.getAs[String]("studentId"))).distinct

    val caExamIdAndStudentIdInfo = caExamIDsOfSemIdDF.select(
      "examId,studentId".split(",").toSeq.map(col): _*
    ).distinct.collect.toSeq.map(x => (x.getAs[String]("examId"),
      x.getAs[String]("studentId")))

    val caGoldInfo = spark.read.format("delta").load(inputMap("goldCAPath"))

    val saGoldInfo = spark.read.format("delta").load(inputMap("goldSAPath"))
    /*
    caGoldInfo.dtypes.map(x => x._2 match {
     case value if value.toLowerCase.contains("string") => s"""x.getAs[String]("${x._1}")"""
         case value if value.toLowerCase.contains("int") => s"""x.getAs[Int]("${x._1}")"""
     case value if value.toLowerCase.contains("decimal") => s"""x.getAs[java.util.BigDecimal]("${x._1}")"""
    })*/

    val saRecordsForIncomingKeysDF = saGoldInfo
      .where(s"examId in ${
        getWhereCondition(
          saExamIdAndStudentIdInfo.map(_._1).toArray)
      } and studentId in ${
        getWhereCondition(
          saExamIdAndStudentIdInfo.map(_._2).toArray)
      }").withColumn("examType", lit(summativeAssessment))
    //  .as("readFromGold")
    /*      .join(saGoldInfo
        .where(s"examId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._1).toArray)} and studentId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._2).toArray)}").groupBy("studentId").agg(countDistinct("examId").as("examsAttended"))
        .as("readFromGoldInner") , col("readFromGoldInner.studentId") === col("readFromGold.studentId")
      ).select("readFromGold.*","examsAttended")*/

    saRecordsForIncomingKeysDF.withColumn("saRecordsForIncomingKeysDF", lit("saRecordsForIncomingKeysDF")).show(false)

    val caRecordsForIncomingKeysDF = caGoldInfo
      .where(s"examId in ${
        getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._1).toArray)
      } and studentId in ${
        getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._2).toArray)
      }").withColumn("examType", lit(cumulativeAssessment))

    /*
    val caRecordsForIncomingKeysDFTmp=caRecordsForIncomingKeysDF.filter(s"examId in ('e001')")
    val caRecordsForIncomingKeysDF=caRecordsForIncomingKeysDFTmp
    * */
    // .as("readFromGold")
    /* .join(caGoldInfo
        .where(s"examId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._1).toArray)} and studentId in ${getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._2).toArray)}").groupBy("studentId").agg(countDistinct("examId").as("examsAttended"))
        .as("readFromGoldInner") , col("readFromGoldInner.studentId") === col("readFromGold.studentId")
        ).select("readFromGold.*","examsAttended")
*/


    caRecordsForIncomingKeysDF.withColumn("caRecordsForIncomingKeysDF", lit("caRecordsForIncomingKeysDF")).show(false)
    // atleast 1 ca must be attended, doesn't matter fail or pass.

    ///// else just read gold SA and CA and join with incoming studId
    // and ExamId and filter subTotal and calc. But this gives inconsistent results.
    // SEM logic 60 % from SA (1 Exam usually) , 40 % from (CA's , usually 2 to 3 exams taken in average)

    // logic 1  read separately , join method
/*

    val caMarksCalculated = caRecordsForIncomingKeysDF.join(caExamIDsOfSemIdDF
      , Seq("examId", "studentId", "subjectCode", "examType"), "right")
      .withColumn("maxMarksOfAssessmentCalculated", lit(40.0) / col("num_of_assessments"))
      .withColumn("percentageOfMarksObtained", round((col("marks") * (lit(100.0) / col("maxMarks"))), 3))
      .withColumn("marksObtainedAsPerNewMaxMarks", round(col("percentageOfMarksObtained") * (col("maxMarksOfAssessmentCalculated") / lit(100.0)), 3))
      .na.fill("NA").na.fill(0.0).withColumn("numAssessmentsAttended",
      when(col("grade") === lit("NA") &&
        col("result") === lit("NA"), "-1").otherwise(lit(0)))
      .withColumn("marksObtained", sum("marksObtainedAsPerNewMaxMarks").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", max("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "examId", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", sum("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", (col("numAssessmentsAttended") + col("num_of_assessments"))
        .cast(IntegerType))
      .withColumn("rankCol", dense_rank.over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
          .orderBy(asc("examId"), asc("assessmentYear"))
      )).where(col("rankCol") === lit(1))
      .select("semId|studentId|subjectCode|examType|marksObtained|num_of_assessments|numAssessmentsAttended".split("\\|").map(col).toSeq: _*)
      .orderBy("semId", "studentId", "subjectCode")

     caMarksCalculated.show(false)
   */ /*

      .groupBy("semId","examType","studentId","subjectCode","num_of_assessments")
      .agg(sum("marksObtainedAsPerNewMaxMarks").as("marksObtained"),
       max("numAssessmentsAttended" ).as("numAssessmentsAttended"))
      .withColumn("numAssessmentsAttended",col("numAssessmentsAttended") + col("num_of_assessments"))
      .na.fill("NA").na.fill(0.0).orderBy("semId","studentId","subjectCode")
*/

 /*   val saMarksCalculated = saRecordsForIncomingKeysDF.join(saExamIDsOfSemIdDF
      , Seq("examId", "studentId", "subjectCode", "examType"), "right")
      .withColumn("maxMarksOfAssessmentCalculated", lit(60.0) / col("num_of_assessments"))
      .withColumn("percentageOfMarksObtained", round((col("marks") * (lit(100.0) / col("maxMarks"))), 3))
      .withColumn("marksObtainedAsPerNewMaxMarks", round(col("percentageOfMarksObtained") * (col("maxMarksOfAssessmentCalculated") / lit(100.0)), 3))
      .na.fill("NA").na.fill(0.0).withColumn("numAssessmentsAttended",
      when(col("grade") === lit("NA") &&
        col("result") === lit("NA"), "-1").otherwise(lit(0)))
      .withColumn("marksObtained", sum("marksObtainedAsPerNewMaxMarks").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", max("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "examId", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", sum("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", (col("numAssessmentsAttended") + col("num_of_assessments"))
        .cast(IntegerType))
      .withColumn("rankCol", dense_rank.over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
          .orderBy(asc("examId"), asc("assessmentYear"))
      )).where(col("rankCol") === lit(1))
      .select("semId|studentId|subjectCode|examType|marksObtained|num_of_assessments|numAssessmentsAttended".split("\\|").map(col).toSeq: _*)
      .orderBy("semId", "studentId", "subjectCode")

    saMarksCalculated.show(false)
*/


    /*val resultDF = getMarksCalculatedJoin(saRecordsForIncomingKeysDF,saExamIDsOfSemIdDF).as("sa")
      .join(getMarksCalculatedJoin(caRecordsForIncomingKeysDF,caExamIDsOfSemIdDF).as("ca"),
        Seq("subjectCode", "semId", "studentId"))
      .withColumn("passMarkForSA", round(lit(50.0) * (lit(60.0) / lit(100.0)), 3))
      .withColumn("passMarkForSem", round(lit(60.0), 3))
      .withColumn("finalMarks",
        round(col("sa.marksObtained") + col("ca.marksObtained"), 3))
      .withColumn("result", when(
        col("sa.marksObtained") >= col("passMarkForSA")
          && col("ca.numAssessmentsAttended") > lit(0)
          && col("finalMarks") >= col("passMarkForSem")
        , lit("PASS")).otherwise(lit("FAIL")))
      .withColumn("grade", getGradeUDF(lit(new java.math.BigDecimal(100.0)), col("finalMarks").cast(DecimalType(6, 3))
        , lit("finalCalculation")))
      .withColumn("remarks", when(col("result") === lit("PASS")
        , lit("Keep pushing")).when(col("result") === lit("FAIL") &&
        (col("sa.marksObtained") > col("passMarkForSA") ||
          col("sa.marksObtained") === col("passMarkForSA")) &&
        col("finalMarks") < col("passMarkForSem"),
        lit("Cleared SA, but inadequate marks in CA"))
        .when(col("result") === lit("FAIL") &&
          (col("sa.numAssessmentsAttended") === lit(0) ||
            col("ca.numAssessmentsAttended") === lit(0)),
          lit("Not appeared in adequate exams, Kindly maintain cognizance towards assessments"))
        .when(col("result") === lit("FAIL") &&
          col("sa.marksObtained") > col("passMarkForSA") &&
          col("finalMarks") > col("passMarkForSem")
          &&
          col("sa.numAssessmentsAttended") == lit(0),
          lit("Cleared SA, but have not even attended CA")
        ).when(col("result") === lit("FAIL") &&
        col("sa.marksObtained") < col("passMarkForSA"),
        lit("Failed in SA")).otherwise("NA"))
      .select("subjectCode,semId,studentId,passMarkForSA,passMarkForSem,finalMarks,remarks,grade,result"
        .split(",").map(col).toSeq ++
        Seq(col("sa.marksObtained").as("SA_marks")
          , col("ca.marksObtained").alias("CA_marks"),
          col("ca.numAssessmentsAttended").as("CA_appeared"),
          col("sa.numAssessmentsAttended").as("SA_appeared"),
          col("ca.num_of_assessments").as("totalAssessmentsInCA"),
          col("sa.num_of_assessments").as("totalAssessmentsInSA")): _*)

    resultDF.show(false)
*/
    val finalResultDF =getFinalResultDFJoin(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF)
    /*
    resultDF.select(col("subjectCode"), col("semId"), col("studentId"), col("passMarkForSA"), col("passMarkForSem"), col("finalMarks"), col("remarks"), col("grade"), col("result"), col("SA_marks"), col("CA_marks"), col("CA_appeared"), col("SA_appeared"), col("totalAssessmentsInCA"), col("totalAssessmentsInSA"))
      .union(resultDF.groupBy("studentId", "semId").agg(
        sum("passMarkForSA").as("passMarkForSA")
        , sum("passMarkForSem").as("passMarkForSem"),
        sum("finalMarks").as("finalMarks"),
        collect_list(col("result")).as("resultTmp"),
        collect_list(concat(col("result"), lit("~"), col("subjectCode"))).as("remarksTmp")
        , sum("SA_marks").as("SA_marks")
        , sum("CA_marks").as("CA_marks"),
        max("CA_appeared").as("CA_appeared"),
        max("SA_appeared").as("SA_appeared"),
        max("totalAssessmentsInCA").as("totalAssessmentsInCA"),
        max("totalAssessmentsInSA").as("totalAssessmentsInSA"))
        .withColumn("subjectCode", lit("subTotal"))
        .withColumn("result", when(array_contains(col("resultTmp"), lit("FAIL")),
          lit("Reappear"))
          .otherwise(lit("ALL-CLEAR")))
        .withColumn("remarksColTmp", arrayFilterEqualsUDF(col("resultTmp")
          , lit("FAIL")))
        .withColumn("remarks",
          when(size(col("remarksColTmp")) > lit(0), concat(lit("Failed in "), concat_ws("", split(concat_ws(",",
            arrayFilterContainsUDF(col("remarksTmp"), lit("FAIL"))), "FAIL~"))))
            .otherwise(lit("Keep pushing")))
        .select(col("subjectCode"), col("semId"), col("studentId"), col("passMarkForSA"), col("passMarkForSem"), col("finalMarks"), col("remarks"),
          getGradeUDF(lit(new java.math.BigDecimal(100.0)), col("finalMarks"), lit("finalCalculation")).as("grade")
          , col("result"), col("SA_marks"), col("CA_marks"), col("CA_appeared"), col("SA_appeared"), col("totalAssessmentsInCA"), col("totalAssessmentsInSA"))
      )
*/
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  //  saveDF(finalResultDF.write.mode("overwrite").format("delta").partitionBy("semId", "studentId"), inputMap("diamondPath"))
    finalResultDF.withColumn("finalResultDF",lit("finalResultDF")).show(false)

    // 2nd method using mapgroups

/*

    val saMarksCalculatedMapGroupsDF = saRecordsForIncomingKeysDF.join(saExamIDsOfSemIdDF, Seq("examId", "studentId", "subjectCode", "examType"), "right")
      .na.fill(0).na.fill("NA")
      .groupByKey(x => (x.getAs[String]("semId"), x.getAs[String]("studentId"))).flatMapGroups((keys, rows) => {
      val rowList = rows.toList
      // val examIds=rowList.map(_.getAs[String]("examId")).distinct
      val numberOfExamIds = rowList.head.getAs[Int]("num_of_assessments")
      val maxMarkPerExamId = getMaxMarks("SA") / numberOfExamIds

      val numberOfExamsAttended = rowList.filter(x => x.getAs[String]("assessmentYear") != "NA"
        && x.getAs[String]("grade") != "NA"
        && x.getAs[String]("result") != "NA"
        && x.getAs[String]("comment") != "NA"
        && List(1, -1).contains(x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)))
        && x.getAs[Int]("passMarkPercentage") != 0
        && x.getAs[Int]("maxMarks") != 0
      ).map(_.getAs[String]("examId")).distinct

      println(s"numberOfExamsAttended ${numberOfExamsAttended}")

      val examsNotAttended = rowList /*.map(x => {
        println(s"x ${x}")
        println(s"""x.getAs[String]("assessmentYear") == "NA" ${ x.getAs[String]("assessmentYear") == "NA"}""")
        println(s"""x.getAs[String]("grade") == "NA" ${ x.getAs[String]("grade") == "NA"}""")
        println(s"""x.getAs[String]("result") == "NA" ${x.getAs[String]("result") == "NA"}""")
        println(s"""x.getAs[String]("comment") == "NA" ${ x.getAs[String]("comment") == "NA"}""")
        println(s"""x.getAs[java.math.BigDecimal]("marks") == new java.math.BigDecimal(0.000) ${ x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000))==0}""")
        println(s"""x.getAs[java.math.BigDecimal]("passMarkPercentage") == new java.math.BigDecimal(0) ${ x.getAs[Int]("passMarkPercentage") ==0}""")
        println(s"""x.getAs[java.math.BigDecimal]("maxMarks") ${ x.getAs[Int]("maxMarks")  ==0}""")
        x
      })*/ .filter(x => x.getAs[String]("assessmentYear") == "NA"
        && x.getAs[String]("grade") == "NA"
        && x.getAs[String]("result") == "NA"
        && x.getAs[String]("comment") == "NA"
        && x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)) == 0
        && x.getAs[Int]("passMarkPercentage") == 0
        && x.getAs[Int]("maxMarks") == 0
      ).distinct.map(_.getAs[String]("examId")).distinct

      println(s"numberOfExamsNotAttended ${examsNotAttended}")

      numberOfExamIds == examsNotAttended.size match {
        case true =>
          rowList.groupBy(_.getAs[String]("subjectCode")).map(recordsAndKey =>
            Row(recordsAndKey._2.head.getAs[String]("semId"), //semId
              recordsAndKey._2.head.getString(1), // studentId
              recordsAndKey._2.head.getString(2), // subjectCode
              recordsAndKey._2.head.getString(4), // assessmentYear
              new java.math.BigDecimal(0.0), // aggMarks
              recordsAndKey._2.head.getString(3), // examType
              new java.math.BigDecimal(100.0).divide(new java.math.BigDecimal(100.0), MathContext.DECIMAL128).multiply(new java.math.BigDecimal(50.0), MathContext.DECIMAL128), // aggPassMarks
              new java.math.BigDecimal(100.0), // max marks Agg
              "FAIL", // result
              getGradeJava(new java.math.BigDecimal(100.0), new java.math.BigDecimal(0.0), "SA"), // grade
              numberOfExamIds, //total Number of exams
              0, "NA") // exams Appeared
          )
        case false =>
          // calculate fo valid exams, and update attended count in last count
          rowList.filterNot(x => examsNotAttended.contains(x.getAs[String]("examId")) ).map(x => Row(
            x.getAs[String]("semId"),
            x.getAs[String]("examId"),
            x.getAs[String]("studentId"),
            x.getAs[String]("subjectCode"),
            x.getAs[String]("assessmentYear"),
            x.getAs[java.math.BigDecimal]("marks"),
            // marks percentage
            x.getAs[java.math.BigDecimal]("marks").multiply(new java.math.BigDecimal(100).divide(new java.math.BigDecimal(x.getAs[Int]("maxMarks")), MathContext.DECIMAL128), MathContext.DECIMAL128),
            x.getAs[String]("examType"),
            x.getAs[String]("grade"),
            x.getAs[String]("result"),
            x.getAs[Int]("passMarkPercentage"),
            x.getAs[Int]("maxMarks"),
            maxMarkPerExamId,
            // new marks calculated
            new java.math.BigDecimal(maxMarkPerExamId / 100.0).multiply(x.getAs[java.math.BigDecimal]("marks").multiply(new java.math.BigDecimal(100).divide(new java.math.BigDecimal(x.getAs[Int]("maxMarks")), MathContext.DECIMAL128))),
            x.getAs[Int]("passMarkCalculated"),
            // new pass marks calculated
            new java.math.BigDecimal((maxMarkPerExamId / 100.0) * x.getAs[Int]("passMarkPercentage")),
            x.getAs[String]("comment"),
            numberOfExamIds,
            numberOfExamsAttended.size)).groupBy(x => (x.getAs[String](0),x.getAs[String](2),x.getAs[String](3)))
            .map(examPerSubject => {
            val marksTotalPerSubID = {
              for (record <- examPerSubject._2) yield record.getAs[java.math.BigDecimal](13)
            }.reduceRight /*(new java.math.BigDecimal(0.0))*/ ((x, y) => x.add(y, MathContext.DECIMAL128))
            val passMarksTotalPerSubID = {
              for (record <- examPerSubject._2) yield record.getAs[java.math.BigDecimal](15)
            }.reduce[java.math.BigDecimal]((x, y) => x.add(y, MathContext.DECIMAL128))

            Row(
              examPerSubject._2.head.getString(0) , //semID
              examPerSubject._2.head.getString(2), // studentId
              examPerSubject._2.head.getString(3), // subjectCode
              examPerSubject._2.head.getString(4), // assessmentYear
              marksTotalPerSubID, // aggMarks
              examPerSubject._2.head.getString(7), // examType
              passMarksTotalPerSubID, // aggPassMarks
              new java.math.BigDecimal(100.0), // max marks Agg
              marksTotalPerSubID match { case value if value.compareTo(passMarksTotalPerSubID) == -1 => "FAIL" case value if List(0, 1).contains(value.compareTo(passMarksTotalPerSubID)) => "PASS" }, // result
              getGradeJava(new java.math.BigDecimal(100.0), marksTotalPerSubID, "SA"), // grade
              examPerSubject._2.head.getInt(17), //total Number of exams
              examPerSubject._2.head.getInt(18), // exams Appeared
              "")  // comment
          })
      }
    })(RowEncoder(new StructType(Array(StructField("semId", StringType, true)
      , StructField("studentId", StringType, true)
      , StructField("subjectCode", StringType, true)
      , StructField("assessmentYear", StringType, true)
      , StructField("aggMarks", DecimalType(6, 3), true)
      , StructField("examType", StringType, true)
      , StructField("AggPassMark", DecimalType(6, 3), true)
      , StructField("maxMarksAgg", DecimalType(6, 3), true),
      StructField("result", StringType, true)
      , StructField("grade", StringType, true)
      , StructField("totalNumberOfExams", IntegerType, true),
      StructField("examsAttended", IntegerType, true),
      StructField("comment", StringType, true))))) /*.groupByKey(_.getAs[String]("studentId"))
      .flatMapGroups( (key, rows) => {
      val rowsTmp = rows.toList
      val totalMarksScored=rowsTmp.map(_.getAs[java.math.BigDecimal]("aggMarks")).foldLeft(new java.math.BigDecimal(0.000))((x,y)=> x.add(y,MathContext.DECIMAL128))
      val totalMaxMarks=  {for(rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal]("maxMarksAgg") }.reduce((x,y)=> x.add(y,MathContext.DECIMAL128))
      val aggPassMark={for(rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal]("AggPassMark") }.foldRight(new java.math.BigDecimal(0.000))((x,y)=> x.add(y,MathContext.DECIMAL128))
      val failPresentCondition=rowsTmp.map(_.getAs[String]("result")).contains("FAIL")

        rowsTmp :+ Row(rowsTmp.head.getAs[String]("examId"),
          rowsTmp.head.getAs[String]("studentId"),
          "subTotal",
          rowsTmp.head.getAs[String]("assessmentYear"),
          totalMarksScored,
          rowsTmp.head.getAs[String]("examType"),
          aggPassMark,
          totalMaxMarks,
          totalMarksScored match { case value if failPresentCondition == false && List(0,1).contains(value.compareTo( aggPassMark)) => "PASS" case _ => "FAIL"},
          getGrade(totalMaxMarks,totalMarksScored,"SA"),
          rowsTmp.head.getAs[Int]("totalNumberOfExams"),
          rowsTmp.head.getAs[Int]("examsAttended"),
          rowsTmp.head.getAs[String]("Comment")
        )
    }
    )*/ .groupByKey(x=> (x.getAs[String](0),x.getAs[String](1)))
      .flatMapGroups((key, rows) => {
        val rowsTmp = rows.toList
        val totalMarksScored = rowsTmp.map(_.getAs[java.math.BigDecimal](4)).foldLeft(new java.math.BigDecimal(0.000))((x, y) => x.add(y, MathContext.DECIMAL128))
        val totalMaxMarks = {
          for (rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal](7)
        }.reduce((x, y) => x.add(y, MathContext.DECIMAL128))
        val aggPassMark = {
          for (rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal](6)
        }.foldRight(new java.math.BigDecimal(0.000))((x, y) => x.add(y, MathContext.DECIMAL128))
        val failPresentCondition = rowsTmp.map(_.getAs[String](9)).contains("FAIL")

        rowsTmp :+ Row(rowsTmp.head.getAs[String](0),
          rowsTmp.head.getAs[String](1),
          "subTotal",
          rowsTmp.head.getAs[String](3),
          totalMarksScored,
          rowsTmp.head.getAs[String](5),
          aggPassMark,
          totalMaxMarks,
          totalMarksScored match { case value if failPresentCondition == false && List(0, 1).contains(value.compareTo(aggPassMark)) => "PASS" case _ => "FAIL" },
          getGrade(totalMaxMarks, totalMarksScored, "SA"),
          rowsTmp.head.getAs[Int](10),
          rowsTmp.head.getAs[Int](11),
          rowsTmp.head.getAs[String](12)
        )
      }
      )(RowEncoder(new StructType(Array(StructField("semId", StringType, true)
        , StructField("studentId", StringType, true)
        , StructField("subjectCode", StringType, true)
        , StructField("assessmentYear", StringType, true)
        , StructField("aggMarks", DecimalType(6, 3), true)
        , StructField("examType", StringType, true)
        , StructField("AggPassMark", DecimalType(6, 3), true)
        , StructField("maxMarksAgg", DecimalType(6, 3), true),
        StructField("result", StringType, true)
        , StructField("grade", StringType, true)
        , StructField("totalNumberOfExams", IntegerType, true),
        StructField("examsAttended", IntegerType, true),
        StructField("comment", StringType, true)))))


    val caMarksCalculatedMapGroupsDF = caRecordsForIncomingKeysDF.join(caExamIDsOfSemIdDF, Seq("examId", "studentId", "subjectCode", "examType"), "right")
      .na.fill(0).na.fill("NA")
      .groupByKey(x => (x.getAs[String]("semId"), x.getAs[String]("studentId"))).flatMapGroups((keys, rows) => {
      val rowList = rows.toList
      val examIds = rowList.map(_.getAs[String]("examId")).distinct
      val numberOfExamIds = examIds.size
      val maxMarkPerExamId =getMaxMarks("CA") / numberOfExamIds

      val numberOfExamsAttended = rowList.filter(x => x.getAs[String]("assessmentYear") != "NA"
        && x.getAs[String]("grade") != "NA"
        && x.getAs[String]("result") != "NA"
        && x.getAs[String]("comment") != "NA"
        && List(1, -1).contains(x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)))
        && x.getAs[Int]("passMarkPercentage") != 0
        && x.getAs[Int]("maxMarks") != 0
      ).map(_.getAs[String]("examId")).distinct

      println(s"numberOfExamsAttended ${numberOfExamsAttended}")

      val numberOfExamsNotAttended = rowList.filter(x => x.getAs[String]("assessmentYear") == "NA"
        && x.getAs[String]("grade") == "NA"
        && x.getAs[String]("result") == "NA"
        && x.getAs[String]("comment") == "NA"
        && x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)) == 0
        && x.getAs[Int]("passMarkPercentage") == 0
        && x.getAs[Int]("maxMarks") == 0
      ).distinct.map(_.getAs[String]("examId")).distinct

      println(s"numberOfExamsNotAttended ${numberOfExamsNotAttended}")

      numberOfExamIds == numberOfExamsNotAttended.size match {
        case true =>
          rowList.groupBy(_.getAs[String]("subjectCode")).map(recordsAndKey =>
            Row(recordsAndKey._2.head.getAs[String]("semId"), //semId
              recordsAndKey._2.head.getString(1), // studentId
              recordsAndKey._2.head.getString(2), // subjectCode
              recordsAndKey._2.head.getString(4), // assessmentYear
              new java.math.BigDecimal(0.0), // aggMarks
              recordsAndKey._2.head.getString(3), // examType
              new java.math.BigDecimal(60.0).divide(new java.math.BigDecimal(100.0), MathContext.DECIMAL128).multiply(new java.math.BigDecimal(45.0), MathContext.DECIMAL128), // aggPassMarks
              new java.math.BigDecimal(60.0), // max marks Agg
              "FAIL", // result
              getGradeJava(new java.math.BigDecimal(60.0), new java.math.BigDecimal(0.0), "CA"), // grade
              numberOfExamIds, //total Number of exams
              0, "NA") // exams Appeared
          )
        case false =>
          rowList.map(x => Row(
            x.getAs[String]("semId"),
            x.getAs[String]("examId"),
            x.getAs[String]("studentId"),
            x.getAs[String]("subjectCode"),
            x.getAs[String]("assessmentYear"),
            x.getAs[java.math.BigDecimal]("marks"),
            // marks percentage
            (x.getAs[java.math.BigDecimal]("marks"), x.getAs[Int]("maxMarks")) match {
              case (_, 0) => new java.math.BigDecimal(0.000)
              case (tempVal, _) if tempVal.compareTo(new java.math.BigDecimal(0.000)) == 0 => new java.math.BigDecimal(0.000)
              case (_, _) =>
                x.getAs[java.math.BigDecimal]("marks").multiply(new java.math.BigDecimal(100).divide(new java.math.BigDecimal(x.getAs[Int]("maxMarks")), MathContext.DECIMAL128), MathContext.DECIMAL128)
            },
            x.getAs[String]("examType"),
            x.getAs[String]("grade"),
            x.getAs[String]("result"),
            x.getAs[Int]("passMarkPercentage"),
            x.getAs[Int]("maxMarks"),
            maxMarkPerExamId,
            // new marks calculated
            (x.getAs[java.math.BigDecimal]("marks"), x.getAs[Int]("maxMarks")) match {
              case (_, 0) => new java.math.BigDecimal(0.000)
              case (tempVal, _) if tempVal.compareTo(new java.math.BigDecimal(0.000)) == 0 => new java.math.BigDecimal(0.000)
              case (_, _) =>
                new java.math.BigDecimal(maxMarkPerExamId / 100.0).multiply(x.getAs[java.math.BigDecimal]("marks").multiply(new java.math.BigDecimal(100).divide(new java.math.BigDecimal(x.getAs[Int]("maxMarks")), MathContext.DECIMAL128)))
            },
            x.getAs[Int]("passMarkCalculated"),
            // new pass marks calculated
            new java.math.BigDecimal((maxMarkPerExamId / 100.0) * getPassMarkPercentage("CA")),
            x.getAs[String]("comment"),
            numberOfExamIds,
            numberOfExamsAttended.size)).groupBy(x => (x.getAs[String](0),x.getAs[String](2),x.getAs[String](3))).map(examPerSubject => {

            val marksTotalPerSubID = {
              for (record <- examPerSubject._2) yield record.getAs[java.math.BigDecimal](13)
            }.reduceRight /*(new java.math.BigDecimal(0.0))*/ ((x, y) => x.add(y, MathContext.DECIMAL128))
            val passMarksTotalPerSubID = {
              for (record <- examPerSubject._2) yield record.getAs[java.math.BigDecimal](15)
            }.reduce[java.math.BigDecimal]((x, y) => x.add(y, MathContext.DECIMAL128))
            Row(examPerSubject._2.head.getString(0), // semId
              examPerSubject._2.head.getString(2), // studentId
              examPerSubject._2.head.getString(3), // subjectCode
              examPerSubject._2.head.getString(4), // assessmentYear
              marksTotalPerSubID, // aggMarks
              examPerSubject._2.head.getString(7), // examType
              passMarksTotalPerSubID, // aggPassMarks
              new java.math.BigDecimal(60.0), // max marks Agg
              marksTotalPerSubID match { case value if value.compareTo(passMarksTotalPerSubID) == -1 => "FAIL" case value if List(0, 1).contains(value.compareTo(passMarksTotalPerSubID)) => "PASS" }, // result
              getGradeJava(new java.math.BigDecimal(60.0), marksTotalPerSubID, "CA"), // grade
              examPerSubject._2.head.getInt(17), //total Number of exams
              examPerSubject._2.head.getInt(18), // exams Appeared
              "") // comment
          })
      }
    })(RowEncoder(new StructType(Array(StructField("semId", StringType, true)
      , StructField("studentId", StringType, true)
      , StructField("subjectCode", StringType, true)
      , StructField("assessmentYear", StringType, true)
      , StructField("aggMarks", DecimalType(6, 3), true)
      , StructField("examType", StringType, true)
      , StructField("AggPassMark", DecimalType(6, 3), true)
      , StructField("maxMarksAgg", DecimalType(6, 3), true),
      StructField("result", StringType, true)
      , StructField("grade", StringType, true)
      , StructField("totalNumberOfExams", IntegerType, true),
      StructField("examsAttended", IntegerType, true),
      StructField("comment", StringType, true))))).groupByKey(x=>(x.getAs[String](0),x.getAs[String](1)))
      .flatMapGroups((key, rows) => {
        val rowsTmp = rows.toList
         val totalMarksScored = rowsTmp.map(_.getAs[java.math.BigDecimal](4)).foldLeft(new java.math.BigDecimal(0.000))((x, y) => x.add(y, MathContext.DECIMAL128))
        val totalMaxMarks = {
          for (rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal](7)
        }.reduce((x, y) => x.add(y, MathContext.DECIMAL128))
        val aggPassMark = {
          for (rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal](6)
        }.foldRight(new java.math.BigDecimal(0.000))((x, y) => x.add(y, MathContext.DECIMAL128))
        val failPresentCondition = rowsTmp.map(_.getAs[String](8)).contains("FAIL")

        rowsTmp :+ Row(rowsTmp.head.getAs[String](0),
          rowsTmp.head.getAs[String](1),
          "subTotal",
          rowsTmp.head.getAs[String](3),
          totalMarksScored,
          rowsTmp.head.getAs[String](5),
          aggPassMark,
          totalMaxMarks,
          totalMarksScored match { case value if failPresentCondition == false && List(0, 1).contains(value.compareTo(aggPassMark)) => "PASS" case _ => "FAIL" },
          getGradeJava(totalMaxMarks, totalMarksScored, "CA"),
          rowsTmp.head.getAs[Int](10),
          rowsTmp.head.getAs[Int](11),
          rowsTmp.head.getAs[String](12)
        )
      }
      )(RowEncoder(new StructType(Array(StructField("semId", StringType, true)
        , StructField("studentId", StringType, true)
        , StructField("subjectCode", StringType, true)
        , StructField("assessmentYear", StringType, true)
        , StructField("aggMarks", DecimalType(6, 3), true)
        , StructField("examType", StringType, true)
        , StructField("AggPassMark", DecimalType(6, 3), true)
        , StructField("maxMarksAgg", DecimalType(6, 3), true),
        StructField("result", StringType, true)
        , StructField("grade", StringType, true)
        , StructField("totalNumberOfExams", IntegerType, true),
        StructField("examsAttended", IntegerType, true),
        StructField("comment", StringType, true)))))
*/
/*

    val resultMapGroupDF = saMarksCalculatedMapGroupsDF.as("sa")
      .join(caMarksCalculatedMapGroupsDF.as("ca"), "semId,studentId,subjectCode".split(",").toSeq).select(
       "semId,studentId,subjectCode".split(",").map(col) ++
        getCASACols( saMarksCalculatedMapGroupsDF.columns.diff("semId,studentId,subjectCode".split(",")))  :_*
    ).groupByKey(x => (x.getAs[String]("semId"),x.getAs[String]("studentId"),x.getAs[String]("subjectCode")))
      .mapGroups((keys,rows) => {
        /*
        semId: string (nullable = true)
 |-- studentId: string (nullable = true)
 |-- subjectCode: string (nullable = true)
 |-- sa_assessmentYear: string (nullable = true)
 |-- ca_assessmentYear: string (nullable = true)
 |-- sa_aggMarks: decimal(6,3) (nullable = true)
 |-- ca_aggMarks: decimal(6,3) (nullable = true)
 |-- sa_examType: string (nullable = true)
 |-- ca_examType: string (nullable = true)
 |-- sa_AggPassMark: decimal(6,3) (nullable = true)
 |-- ca_AggPassMark: decimal(6,3) (nullable = true)
 |-- sa_maxMarksAgg: decimal(6,3) (nullable = true)
 |-- ca_maxMarksAgg: decimal(6,3) (nullable = true)
 |-- sa_result: string (nullable = true)
 |-- ca_result: string (nullable = true)
 |-- sa_grade: string (nullable = true)
 |-- ca_grade: string (nullable = true)
 |-- sa_totalNumberOfExams: integer (nullable = true)
 |-- ca_totalNumberOfExams: integer (nullable = true)
 |-- sa_examsAttended: integer (nullable = true)
 |-- ca_examsAttended: integer (nullable = true)
 |-- sa_comment: string (nullable = true)
 |-- ca_comment: str

*/
        val rowsTmp= rows.toList
        val caMarksRecalculated=getPercentage(new java.math.BigDecimal(getMaxMarks("CA")),
          rowsTmp.head.getAs[java.math.BigDecimal]("ca_aggMarks"))
          .multiply(new java.math.BigDecimal(40.0 / 100),MathContext.DECIMAL128)

         val saMarksRecalculated=getPercentage(new java.math.BigDecimal(getMaxMarks("SA")),
           rowsTmp.head.getAs[java.math.BigDecimal]("sa_aggMarks"))
           .multiply(new java.math.BigDecimal(60.0 / 100),MathContext.DECIMAL128)

        val caPassMarkNew= getMarksForPercentage(new java.math.BigDecimal(40.0),new java.math.BigDecimal(45.0) )
        val saPassMarkNew= getMarksForPercentage(new java.math.BigDecimal(60.0),new java.math.BigDecimal(50.0) )

        val totalMarksCalculated=caMarksRecalculated.add(saMarksRecalculated)

        val (semResult,semRemarks)=  totalMarksCalculated match {
          case value if rowsTmp.head.getAs[Int]("ca_examsAttended") == 0 =>
            ("FAIL","Did not appear in any CA")
          case value if rowsTmp.head.getAs[Int]("sa_examsAttended") == 0 =>
            ("FAIL","Did not appear in any SA")
          case value if !checkPass(saMarksRecalculated,saPassMarkNew) =>
            ("FAIL","Failed to clear SA")
          case value if !checkPass(value,new java.math.BigDecimal(60.0)) && checkPass(saMarksRecalculated,saPassMarkNew) =>
            ("FAIL","Failed in sem ,cleared SA")
          case value if checkPass(value,new java.math.BigDecimal(60.0))
          && rowsTmp.head.getAs[Int]("ca_examsAttended") >1  && checkPass(saMarksRecalculated,saPassMarkNew)
          &&  checkPass(caMarksRecalculated,caPassMarkNew) =>
            ("PASS","Keep pushing")
          case value if checkPass(value,new java.math.BigDecimal(60.0))
            && rowsTmp.head.getAs[Int]("ca_examsAttended") >1  && checkPass(saMarksRecalculated,saPassMarkNew)
            &&  !checkPass(caMarksRecalculated,caPassMarkNew) =>
            ("PASS","Keep pushing, try to clear CA")
          case value if checkPass(value,new java.math.BigDecimal(60.0))
            && rowsTmp.head.getAs[Int]("ca_examsAttended") ==1  && checkPass(saMarksRecalculated,saPassMarkNew)
            &&  !checkPass(caMarksRecalculated,caPassMarkNew) =>
            ("PASS","Keep pushing, try to attend and clear all CA")
          case value if checkPass(value,new java.math.BigDecimal(60.0))
            && rowsTmp.head.getAs[Int]("ca_examsAttended") ==1  && checkPass(saMarksRecalculated,saPassMarkNew)
            &&  checkPass(caMarksRecalculated,caPassMarkNew) =>
            ("PASS","Keep pushing, try to attend all CA")
        }
      Row(rowsTmp.head.getAs[String]("semId"),
        rowsTmp.head.getAs[String]("studentId"),
        rowsTmp.head.getAs[String]("subjectCode"),
        caMarksRecalculated,
        saMarksRecalculated,
        caPassMarkNew,
        saPassMarkNew,
        totalMarksCalculated,
        new java.math.BigDecimal(60.0),
        semResult,
        semRemarks,
        getGradeJava(new java.math.BigDecimal(100.0),totalMarksCalculated,"finalCalculation"),
        rowsTmp.head.getAs[Int]("sa_totalNumberOfExams"),
        rowsTmp.head.getAs[Int]("ca_totalNumberOfExams"),
        rowsTmp.head.getAs[Int]("sa_examsAttended"),
        rowsTmp.head.getAs[Int]("ca_examsAttended")
      )
      }) (RowEncoder(new StructType(Array(StructField("semId",StringType,true),
        StructField("studentId",StringType,true),
        StructField("subjectCode",StringType,true),
        StructField("caMarks",DecimalType(6,3),true),
        StructField("saMarks",DecimalType(6,3),true),
        StructField("caPassMarks",DecimalType(6,3),true),
        StructField("saPassMarks",DecimalType(6,3),true),
        StructField("totalMarks",DecimalType(6,3),true),
        StructField("passMarkForTotal",DecimalType(6,3),true),
        StructField("result",StringType,true),
        StructField("remarks",StringType,true),
        StructField("grade",StringType,true),
        StructField("saTotalNumberOfExams",IntegerType,true),
        StructField("caTotalNumberOfExams",IntegerType,true),
        StructField("saExamsAttended",IntegerType,true),
        StructField("caExamsAttended",IntegerType,true)
      )))).groupByKey(x=> (x.getAs[String](0),x.getAs[String](1)))
      .flatMapGroups((key,rows) => {
        val rowsTmp=rows.toList
        val totalRecord=rowsTmp.filter(_.getAs[String](2)=="subTotal").head

        rowsTmp.filter(_.getAs[String](2)!="subTotal") :+ Row(
          totalRecord.getAs[String](0),
          totalRecord.getAs[String](1),
          totalRecord.getAs[String](2),
          totalRecord.getAs[java.math.BigDecimal](3),
          totalRecord.getAs[java.math.BigDecimal](4),
          totalRecord.getAs[java.math.BigDecimal](5),
          totalRecord.getAs[java.math.BigDecimal](6),
          totalRecord.getAs[java.math.BigDecimal](7),
          totalRecord.getAs[java.math.BigDecimal](8),
          rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case true => "Re-Appear for failed subjects" case false => "All clear"} ,
          rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Failed")).size >1 && rowsTmp.map(_.getAs[String](10)).filter(_.contains("Did not")) .size >1 => "Concentrate on failed subjects next time, appear in all exams to increase the chances of clearing" case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Failed")).size >1 => "Concentrate on failed subjects next time" case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Did not")).size >1 => "Appear in all exams to increase the chances of clearing" case false => "Keep pushing"},
          rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case value if value == true && totalRecord.getAs[java.math.BigDecimal](7).compareTo(new java.math.BigDecimal(0)) ==0 => "F-" case true => "F" case false =>totalRecord.getAs[Int](11) },
          totalRecord.getAs[Int](12),
          totalRecord.getAs[Int](13),
          totalRecord.getAs[Int](14),
          totalRecord.getAs[Int](15)
        )
      })(RowEncoder(new StructType(Array(StructField("semId",StringType,true),
        StructField("studentId",StringType,true),
        StructField("subjectCode",StringType,true),
        StructField("caMarks",DecimalType(6,3),true),
        StructField("saMarks",DecimalType(6,3),true),
        StructField("caPassMarks",DecimalType(6,3),true),
        StructField("saPassMarks",DecimalType(6,3),true),
        StructField("totalMarks",DecimalType(6,3),true),
        StructField("passMarkForTotal",DecimalType(6,3),true),
        StructField("result",StringType,true),
        StructField("remarks",StringType,true),
        StructField("grade",StringType,true),
        StructField("saTotalNumberOfExams",IntegerType,true),
        StructField("caTotalNumberOfExams",IntegerType,true),
        StructField("saExamsAttended",IntegerType,true),
        StructField("caExamsAttended",IntegerType,true)
      ))))


      col("alpha.semId") === col("delta.semId") &&
col("alpha.studentId") === col("delta.studentId") &&
col("alpha.subjectCode") === col("delta.subjectCode") &&
col("alpha.caMarks") === col("delta.caMarks") &&
col("alpha.saMarks") === col("delta.saMarks") &&
col("alpha.caPassMarks") === col("delta.caPassMarks") &&
col("alpha.saPassMarks") === col("delta.saPassMarks") &&
col("alpha.totalMarks") === col("delta.totalMarks") &&
col("alpha.passMarkForTotal") === col("delta.passMarkForTotal") &&
col("alpha.result") === col("delta.result") &&
col("alpha.remarks") === col("delta.remarks") &&
col("alpha.grade") === col("delta.grade") &&
col("alpha.saTotalNumberOfExams") === col("delta.saTotalNumberOfExams") &&
col("alpha.caTotalNumberOfExams") === col("delta.caTotalNumberOfExams") &&
col("alpha.saExamsAttended") === col("delta.saExamsAttended") &&
col("alpha.caExamsAttended") === col("delta.caExamsAttended") &&

col("alpha.semId") =!= col("delta.semId") ||
col("alpha.studentId") =!= col("delta.studentId") ||
col("alpha.subjectCode") =!= col("delta.subjectCode") ||
col("alpha.caMarks") =!= col("delta.caMarks") ||
col("alpha.saMarks") =!= col("delta.saMarks") ||
col("alpha.caPassMarks") =!= col("delta.caPassMarks") ||
col("alpha.saPassMarks") =!= col("delta.saPassMarks") ||
col("alpha.totalMarks") =!= col("delta.totalMarks") ||
col("alpha.passMarkForTotal") =!= col("delta.passMarkForTotal") ||
col("alpha.result") =!= col("delta.result") ||
col("alpha.remarks") =!= col("delta.remarks") ||
col("alpha.grade") =!= col("delta.grade") ||
col("alpha.saTotalNumberOfExams") =!= col("delta.saTotalNumberOfExams") ||
col("alpha.caTotalNumberOfExams") =!= col("delta.caTotalNumberOfExams") ||
col("alpha.saExamsAttended") =!= col("delta.saExamsAttended") ||
col("alpha.caExamsAttended") =!= col("delta.caExamsAttended") ||
 */



    // totalmarks, 50 % on SA, and atleast 1 apperance in CA and 60 % in total

      examMarksCalculatedMapGroupResultDF(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF).withColumn("finalMapGroupsDF",lit("finalMapGroupsDF")).show(false)

    examIDSemIDAndTypeMappedDF.unpersist

    scala.util.Try{DeltaTable.forPath(spark,inputMap("diamondPath"))} match {
      case scala.util.Success(s) =>
        s.as("alpha").merge(
          examMarksCalculatedMapGroupResultDF(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF).withColumn("incomingTs",lit(getTs)).as("delta"),
          col("alpha.semId") === col("delta.semId") &&
            col("alpha.studentId") === col("delta.studentId")).whenMatched(
          col("alpha.subjectCode") === col("delta.subjectCode")    &&
            col("alpha.caMarks") === col("delta.caMarks") &&
            col("alpha.saMarks") === col("delta.saMarks") &&
            col("alpha.caPassMarks") === col("delta.caPassMarks") &&
            col("alpha.saPassMarks") === col("delta.saPassMarks") &&
            col("alpha.totalMarks") === col("delta.totalMarks") &&
            col("alpha.passMarkForTotal") === col("delta.passMarkForTotal") &&
            col("alpha.result") === col("delta.result") &&
            col("alpha.remarks") === col("delta.remarks") &&
            col("alpha.grade") === col("delta.grade") &&
            col("alpha.saTotalNumberOfExams") === col("delta.saTotalNumberOfExams") &&
            col("alpha.caTotalNumberOfExams") === col("delta.caTotalNumberOfExams") &&
            col("alpha.saExamsAttended") === col("delta.saExamsAttended") &&
            col("alpha.caExamsAttended") === col("delta.caExamsAttended")
            ).updateExpr(Map("incomingTs"->"alpha.incomingTs")).whenMatched(
          col("alpha.subjectCode") =!= col("delta.subjectCode") ||
            col("alpha.caMarks") =!= col("delta.caMarks") ||
            col("alpha.saMarks") =!= col("delta.saMarks") ||
            col("alpha.caPassMarks") =!= col("delta.caPassMarks") ||
            col("alpha.saPassMarks") =!= col("delta.saPassMarks") ||
            col("alpha.totalMarks") =!= col("delta.totalMarks") ||
            col("alpha.passMarkForTotal") =!= col("delta.passMarkForTotal") ||
            col("alpha.result") =!= col("delta.result") ||
            col("alpha.remarks") =!= col("delta.remarks") ||
            col("alpha.grade") =!= col("delta.grade") ||
            col("alpha.saTotalNumberOfExams") =!= col("delta.saTotalNumberOfExams") ||
            col("alpha.caTotalNumberOfExams") =!= col("delta.caTotalNumberOfExams") ||
            col("alpha.saExamsAttended") =!= col("delta.saExamsAttended") ||
            col("alpha.caExamsAttended") =!= col("delta.caExamsAttended")
        ).updateExpr(Map(
          "subjectCode" -> "delta.subjectCode" ,
          "caMarks" -> "delta.caMarks" ,
          "saMarks" -> "delta.saMarks" ,
          "caPassMarks" -> "delta.caPassMarks" ,
          "saPassMarks" -> "delta.saPassMarks" ,
          "totalMarks" -> "delta.totalMarks" ,
          "passMarkForTotal" -> "delta.passMarkForTotal" ,
          "result" -> "delta.result" ,
          "remarks" -> "delta.remarks" ,
          "grade" -> "delta.grade" ,
          "saTotalNumberOfExams" -> "delta.saTotalNumberOfExams" ,
          "caTotalNumberOfExams" -> "delta.caTotalNumberOfExams" ,
          "saExamsAttended" -> "delta.saExamsAttended" ,
          "caExamsAttended" -> "delta.caExamsAttended" ,
          "incomingTs"->"alpha.incomingTs")).whenNotMatched.insertExpr(Map("semId" -> "delta.semId" ,
          "studentId" -> "delta.studentId" ,
          "subjectCode" -> "delta.subjectCode" ,
          "caMarks" -> "delta.caMarks" ,
          "saMarks" -> "delta.saMarks" ,
          "caPassMarks" -> "delta.caPassMarks" ,
          "saPassMarks" -> "delta.saPassMarks" ,
          "totalMarks" -> "delta.totalMarks" ,
          "passMarkForTotal" -> "delta.passMarkForTotal" ,
          "result" -> "delta.result" ,
          "remarks" -> "delta.remarks" ,
          "grade" -> "delta.grade" ,
          "saTotalNumberOfExams" -> "delta.saTotalNumberOfExams" ,
          "caTotalNumberOfExams" -> "delta.caTotalNumberOfExams" ,
          "saExamsAttended" -> "delta.saExamsAttended" ,
          "caExamsAttended" -> "delta.caExamsAttended" ,
          "incomingTs"->"alpha.incomingTs")).execute

      case scala.util.Failure(f) =>
        println(s"Failure ${f.getMessage}")
        saveDFGeneric[org.apache.spark.sql.Row](
          examMarksCalculatedMapGroupResultDF(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF).withColumn("incomingTs",lit(getTs)).write.format("delta").mode("append").partitionBy("semId","studentId")
          ,inputMap("diamondPath")
        )


    }

  }



  def getMarksCalculatedJoinAdvanced(incomingRecords:org.apache.spark.sql.DataFrame
                                     ,semAndExamIdDF:org.apache.spark.sql.DataFrame)=incomingRecords.as("incoming").
    join(semAndExamIdDF.as("reference"),"examId,subjectCode,studentId,examType".foldLeft(lit(1)===lit(1))(
      (columnVariable,colName) => columnVariable && col(s"incoming.${colName}")===col(s"reference.${colName}"))
      ,"right").
    withColumn("maxMaxMarksCalculated", maxMarksUDF(col("examType"))/col("num_of_assessments")).
    withColumn("marksPercentage",col("marks") * round((lit(100.0) /col("maxMarks")),3).cast(DecimalType(6,3)))
    .withColumn("numberOfExamsAttendedPErExamType",count(when(col("marks").isNull,lit(0)).otherwise(lit(1)))
      .over(Window.partitionBy("studentID","subjectCode","examType")))

  def getMarksCalculatedJoin(incomingRecords:org.apache.spark.sql.DataFrame,semAndExamIdDF:org.apache.spark.sql.DataFrame) =
    incomingRecords.as("incoming").join(semAndExamIdDF.as("reference")
      , Seq("examId", "studentId", "subjectCode", "examType"), "right")
      .withColumn("maxMarksOfAssessmentCalculated", maxMarksUDF(col("examType")) / col("num_of_assessments"))
      .withColumn("percentageOfMarksObtained", round((col("marks") * (lit(100.0) / col("maxMarks"))), 3))
      .withColumn("marksObtainedAsPerNewMaxMarks", round(col("percentageOfMarksObtained") * (col("maxMarksOfAssessmentCalculated") / lit(100.0)), 3))
      .na.fill("NA").na.fill(0.0).withColumn("numAssessmentsAttended",
      when(col("grade") === lit("NA") &&
        col("result") === lit("NA"), "-1").otherwise(lit(0)))
      .withColumn("marksObtained", sum("marksObtainedAsPerNewMaxMarks").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", max("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "examId", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", sum("numAssessmentsAttended").over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
      ))
      .withColumn("numAssessmentsAttended", (col("numAssessmentsAttended") + col("num_of_assessments"))
        .cast(IntegerType))
      .withColumn("rankCol", dense_rank.over(
        org.apache.spark.sql.expressions.Window.partitionBy("semId", "examType", "studentId", "subjectCode")
          .orderBy(asc("examId"), asc("assessmentYear"))
      )).where(col("rankCol") === lit(1))
      .select("semId|studentId|subjectCode|examType|marksObtained|num_of_assessments|numAssessmentsAttended".split("\\|").map(col).toSeq: _*)
      .orderBy("semId", "studentId", "subjectCode")

  def getResultDFJoin(incomingDataRecords:org.apache.spark.sql.DataFrame,examIdsOfSemID:org.apache.spark.sql.DataFrame)=getMarksCalculatedJoin(incomingDataRecords.filter(col("examType") === lit(summativeAssessment)),examIdsOfSemID.filter(col("examType") === lit(summativeAssessment))).as("sa")
    .join(getMarksCalculatedJoin(incomingDataRecords.filter(col("examType") === lit(cumulativeAssessment)),examIdsOfSemID.filter(col("examType") === lit(cumulativeAssessment))).as("ca"),
      Seq("subjectCode", "semId", "studentId"))
    .withColumn("passMarkForSA", round(lit(50.0) * (lit(60.0) / lit(100.0)), 3))
    .withColumn("passMarkForSem", round(lit(60.0), 3))
    .withColumn("finalMarks",
      round(col("sa.marksObtained") + col("ca.marksObtained"), 3))
    .withColumn("result", when(
      col("sa.marksObtained") >= col("passMarkForSA")
        && col("ca.numAssessmentsAttended") > lit(0)
        && col("finalMarks") >= col("passMarkForSem")
      , lit("PASS")).otherwise(lit("FAIL")))
    .withColumn("grade", getGradeUDF(lit(new java.math.BigDecimal(100.0)), col("finalMarks").cast(DecimalType(6, 3))
      , lit("finalCalculation")))
    .withColumn("remarks", when(col("result") === lit("PASS")
      , lit("Keep pushing")).when(col("result") === lit("FAIL") &&
      (col("sa.marksObtained") > col("passMarkForSA") ||
        col("sa.marksObtained") === col("passMarkForSA")) &&
      col("finalMarks") < col("passMarkForSem"),
      lit("Cleared SA, but inadequate marks in CA"))
      .when(col("result") === lit("FAIL") &&
        (col("sa.numAssessmentsAttended") === lit(0) ||
          col("ca.numAssessmentsAttended") === lit(0)),
        lit("Not appeared in adequate exams, Kindly maintain cognizance towards assessments"))
      .when(col("result") === lit("FAIL") &&
        col("sa.marksObtained") > col("passMarkForSA") &&
        col("finalMarks") > col("passMarkForSem")
        &&
        col("sa.numAssessmentsAttended") == lit(0),
        lit("Cleared SA, but have not even attended CA")
      ).when(col("result") === lit("FAIL") &&
      col("sa.marksObtained") < col("passMarkForSA"),
      lit("Failed in SA")).otherwise("NA"))
    .select("subjectCode,semId,studentId,passMarkForSA,passMarkForSem,finalMarks,remarks,grade,result"
      .split(",").map(col).toSeq ++
      Seq(col("sa.marksObtained").as("SA_marks")
        , col("ca.marksObtained").alias("CA_marks"),
        col("ca.numAssessmentsAttended").as("CA_appeared"),
        col("sa.numAssessmentsAttended").as("SA_appeared"),
        col("ca.num_of_assessments").as("totalAssessmentsInCA"),
        col("sa.num_of_assessments").as("totalAssessmentsInSA")): _*)


  def getFinalResultDFJoin(incomingDataRecords:org.apache.spark.sql.DataFrame,examIdsOfSemID:org.apache.spark.sql.DataFrame)=getResultDFJoin(incomingDataRecords,examIdsOfSemID) match
  {
    case resultDF =>
      resultDF.select(col("subjectCode"), col("semId"), col("studentId"), col("passMarkForSA"), col("passMarkForSem"), col("finalMarks"), col("remarks"), col("grade"), col("result"), col("SA_marks"), col("CA_marks"), col("CA_appeared"), col("SA_appeared"), col("totalAssessmentsInCA"), col("totalAssessmentsInSA"))
        .union(resultDF.groupBy("studentId", "semId").agg(
          sum(col("passMarkForSA")).as("passMarkForSA")
          , sum($"passMarkForSem").as("passMarkForSem"),
          round(sum($"finalMarks"),3).as("finalMarks"),
          collect_list(col("result")).as("resultTmp"),
          collect_list(concat(col("result"), lit("~"), col("subjectCode"))).as("remarksTmp")
          , round(sum($"SA_marks"),3).as("SA_marks")
          , round(sum($"CA_marks"),3).as("CA_marks"),
          round(max($"CA_appeared"),3).as("CA_appeared"),
          round(max($"SA_appeared"),3).as("SA_appeared"),
          max("totalAssessmentsInCA").as("totalAssessmentsInCA"),
          max("totalAssessmentsInSA").as("totalAssessmentsInSA"))
          .withColumn("subjectCode", lit("subTotal"))
          .withColumn("result", when(array_contains(col("resultTmp"), lit("FAIL")),
            lit("Reappear"))
            .otherwise(lit("ALL-CLEAR")))
          .withColumn("remarksColTmp", arrayFilterEqualsUDF(col("resultTmp")
            , lit("FAIL")))
          .withColumn("remarks",
            when(size(col("remarksColTmp")) > lit(0), concat(lit("Failed in "), concat_ws("", split(concat_ws(",",
              arrayFilterContainsUDF(col("remarksTmp"), lit("FAIL"))), "FAIL~"))))
              .otherwise(lit("Keep pushing")))
          .select(col("subjectCode"), col("semId"), col("studentId"), col("passMarkForSA"), col("passMarkForSem"), col("finalMarks"), col("remarks"),
            getGradeUDF(lit(new java.math.BigDecimal(100.0)), col("finalMarks"), lit("finalCalculation")).as("grade")
            , col("result"), col("SA_marks"), col("CA_marks"), col("CA_appeared"), col("SA_appeared"), col("totalAssessmentsInCA"), col("totalAssessmentsInSA"))
        )
  }

  def examMarksCalculatedMapGroup(incomingRecordDF:org.apache.spark.sql.DataFrame,referenceDF:org.apache.spark.sql.DataFrame)=incomingRecordDF.join(referenceDF, Seq("examId", "studentId", "subjectCode", "examType"), "right")
    .na.fill(0).na.fill("NA")
    .groupByKey(x => (x.getAs[String]("semId"), x.getAs[String]("studentId"))).flatMapGroups((keys, rows) => {
    val rowList = rows.toList
    // val examIds=rowList.map(_.getAs[String]("examId")).distinct
    val numberOfExamIds = rowList.head.getAs[Int]("num_of_assessments")
    val maxMarkPerExamId = getMaxMarks(rowList.head.getAs[String]("examType")) / numberOfExamIds

    val numberOfExamsAttended = rowList.filter(x => x.getAs[String]("assessmentYear") != "NA"
      && x.getAs[String]("grade") != "NA"
      && x.getAs[String]("result") != "NA"
      && x.getAs[String]("comment") != "NA"
      && List(1, -1).contains(x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)))
      && x.getAs[Int]("passMarkPercentage") != 0
      && x.getAs[Int]("maxMarks") != 0
    ).map(_.getAs[String]("examId")).distinct

    val examsNotAttended = rowList  .filter(x => x.getAs[String]("assessmentYear") == "NA"
      && x.getAs[String]("grade") == "NA"
      && x.getAs[String]("result") == "NA"
      && x.getAs[String]("comment") == "NA"
      && x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)) == 0
      && x.getAs[Int]("passMarkPercentage") == 0
      && x.getAs[Int]("maxMarks") == 0
    ).distinct.map(_.getAs[String]("examId")).distinct

    (examsNotAttended.size ==  numberOfExamIds) && examsNotAttended.intersect(numberOfExamsAttended).size ==0  match {
      case true =>
        rowList.groupBy(_.getAs[String]("subjectCode")).map(recordsAndKey =>
          Row(recordsAndKey._2.head.getAs[String]("semId"), //semId
            recordsAndKey._2.head.getString(1), // studentId
            recordsAndKey._2.head.getString(2), // subjectCode
            recordsAndKey._2.head.getString(4), // assessmentYear
            new java.math.BigDecimal(0.0), // aggMarks
            recordsAndKey._2.head.getString(3), // examType
            new java.math.BigDecimal(getMaxMarks(rowList.head.getAs[String]("examType"))).divide(new java.math.BigDecimal(100.0), MathContext.DECIMAL128).multiply(new java.math.BigDecimal(getPassMarkPercentage(rowList.head.getAs[String]("examType"))), MathContext.DECIMAL128), // aggPassMarks
            new java.math.BigDecimal(getMaxMarks(rowList.head.getAs[String]("examType"))), // max marks Agg
            "FAIL", // result
            getGradeJava(new java.math.BigDecimal(getMaxMarks(rowList.head.getAs[String]("examType"))), new java.math.BigDecimal(0.0), recordsAndKey._2.head.getString(3)), // grade
            numberOfExamIds, //total Number of exams
            0, "NA") // exams Appeared
        )
      case false =>
        // calculate for valid exams, and update attended count in last count
        rowList/*.filterNot(x => examsNotAttended.contains(x.getAs[String]("examId")) )*/.map(x => Row(
          x.getAs[String]("semId"),
          x.getAs[String]("examId"),
          x.getAs[String]("studentId"),
          x.getAs[String]("subjectCode"),
          rowList.filter(r => numberOfExamsAttended.contains(r.getAs[String]("examId")) && r.getAs[String]("assessmentYear") != "NA").head.getAs[String]("assessmentYear") ,
          x.getAs[java.math.BigDecimal]("marks"),
          // marks percentage
          (x.getAs[java.math.BigDecimal]("marks"), x.getAs[Int]("maxMarks")) match {
            case (_, 0) => new java.math.BigDecimal(0.000)
            case (tempVal, _) if tempVal.compareTo(new java.math.BigDecimal(0.000)) == 0 => new java.math.BigDecimal(0.000)
            case (_, _) =>
              x.getAs[java.math.BigDecimal]("marks").multiply(new java.math.BigDecimal(100).divide(new java.math.BigDecimal(x.getAs[Int]("maxMarks")), MathContext.DECIMAL128), MathContext.DECIMAL128)
          },
          x.getAs[String]("examType"),
          x.getAs[String]("grade"),
          x.getAs[String]("result"),
          x.getAs[Int]("passMarkPercentage"),
          x.getAs[Int]("maxMarks"),
          maxMarkPerExamId,
          // new marks calculated
          (x.getAs[java.math.BigDecimal]("marks"),x.getAs[Int]("maxMarks")) match {
            case (_,0) => new java.math.BigDecimal(0)
            case (tmpVal,_) if tmpVal.compareTo(new java.math.BigDecimal(0.0)) ==0 => new java.math.BigDecimal(0)
            case (_,_) => new java.math.BigDecimal(maxMarkPerExamId / 100.0).multiply(x.getAs[java.math.BigDecimal]("marks").multiply(new java.math.BigDecimal(100).divide(new java.math.BigDecimal(x.getAs[Int]("maxMarks")), MathContext.DECIMAL128)))
          },
          x.getAs[Int]("passMarkCalculated") match {
          case 0 =>getPassMarkPercentage(x.getAs[String]("examType"))
          case value =>  value
        } ,
          // new pass marks calculated
          x.getAs[Int]("passMarkPercentage") match {
            case 0 =>new java.math.BigDecimal((maxMarkPerExamId / 100.0) * getPassMarkPercentage(x.getAs[String]("examType")))
            case _ =>   new java.math.BigDecimal((maxMarkPerExamId / 100.0) * x.getAs[Int]("passMarkPercentage"))
          } ,
          x.getAs[String]("comment"),
          numberOfExamIds,
          rowList.filter(r => x.getAs[String]("examId") == r.getAs[String]("examId") && r.getAs[String]("subjectCode") == x.getAs[String]("subjectCode") && r.getAs[String]("assessmentYear") != "NA").size)).groupBy(x => (x.getAs[String](0),x.getAs[String](2),x.getAs[String](3)))
          .map(examPerSubject => {
            val marksTotalPerSubID = {
              for (record <- examPerSubject._2) yield record.getAs[java.math.BigDecimal](13)
            }.reduceRight /* (new java.math.BigDecimal(0.0)) */ ((x, y) => x.add(y, MathContext.DECIMAL128))
            val passMarksTotalPerSubID = {
              for (record <- examPerSubject._2) yield record.getAs[java.math.BigDecimal](15)
            }.reduce[java.math.BigDecimal]((x, y) => x.add(y, MathContext.DECIMAL128))

            Row(
              examPerSubject._2.head.getString(0) , //semID
              examPerSubject._2.head.getString(2), // studentId
              examPerSubject._2.head.getString(3), // subjectCode
              examPerSubject._2.head.getString(4), // assessmentYear
              marksTotalPerSubID, // aggMarks
              examPerSubject._2.head.getString(7), // examType
              passMarksTotalPerSubID, // aggPassMarks
              new java.math.BigDecimal(getMaxMarks(rowList.head.getAs[String]("examType"))), // max marks Agg
              marksTotalPerSubID match { case value if value.compareTo(passMarksTotalPerSubID) == -1 => "FAIL" case value if List(0, 1).contains(value.compareTo(passMarksTotalPerSubID)) => "PASS" }, // result
              getGradeJava(new java.math.BigDecimal(getMaxMarks(rowList.head.getAs[String]("examType"))), marksTotalPerSubID, examPerSubject._2.head.getString(7)), // grade
              examPerSubject._2.head.getInt(17), //total Number of exams
              examPerSubject._2.head.getInt(18), // exams Appeared
              "")  // comment
          })
    }
  })(RowEncoder(new StructType(Array(StructField("semId", StringType, true)
    , StructField("studentId", StringType, true)
    , StructField("subjectCode", StringType, true)
    , StructField("assessmentYear", StringType, true)
    , StructField("aggMarks", DecimalType(6, 3), true)
    , StructField("examType", StringType, true)
    , StructField("AggPassMark", DecimalType(6, 3), true)
    , StructField("maxMarksAgg", DecimalType(6, 3), true),
    StructField("result", StringType, true)
    , StructField("grade", StringType, true)
    , StructField("totalNumberOfExams", IntegerType, true),
    StructField("examsAttended", IntegerType, true),
    StructField("comment", StringType, true))))) .groupByKey(x=> (x.getAs[String](0),x.getAs[String](1)))
    .flatMapGroups((key, rows) => {
      val rowsTmp = rows.toList
      val totalMarksScored = rowsTmp.map(_.getAs[java.math.BigDecimal](4)).foldLeft(new java.math.BigDecimal(0.000))((x, y) => x.add(y, MathContext.DECIMAL128))
      val totalMaxMarks = {
        for (rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal](7)
      }.reduce((x, y) => x.add(y, MathContext.DECIMAL128))
      val aggPassMark = {
        for (rowTmp <- rowsTmp) yield rowTmp.getAs[java.math.BigDecimal](6)
      }.foldRight(new java.math.BigDecimal(0.000))((x, y) => x.add(y, MathContext.DECIMAL128))
      val failPresentCondition = rowsTmp.map(_.getAs[String](9)).contains("FAIL")

      rowsTmp :+ Row(rowsTmp.head.getAs[String](0),
        rowsTmp.head.getAs[String](1),
        "subTotal",
        rowsTmp.head.getAs[String](3),
        totalMarksScored,
        rowsTmp.head.getAs[String](5),
        aggPassMark,
        totalMaxMarks,
        totalMarksScored match { case value if failPresentCondition == false && List(0, 1).contains(value.compareTo(aggPassMark)) => "PASS" case _ => "FAIL" },
        getGrade(totalMaxMarks, totalMarksScored, rowsTmp.head.getAs[String](5)),
        rowsTmp.head.getAs[Int](10),
        rowsTmp.head.getAs[Int](11),
        rowsTmp.head.getAs[String](12)
      )
    }
    )(RowEncoder(new StructType(Array(StructField("semId", StringType, true)
      , StructField("studentId", StringType, true)
      , StructField("subjectCode", StringType, true)
      , StructField("assessmentYear", StringType, true)
      , StructField("aggMarks", DecimalType(6, 3), true)
      , StructField("examType", StringType, true)
      , StructField("AggPassMark", DecimalType(6, 3), true)
      , StructField("maxMarksAgg", DecimalType(6, 3), true),
      StructField("result", StringType, true)
      , StructField("grade", StringType, true)
      , StructField("totalNumberOfExams", IntegerType, true),
      StructField("examsAttended", IntegerType, true),
      StructField("comment", StringType, true)))))

/*
* convert ca avg to 40 %
* convert sa avg to 60 %
*
* Clear SA and atleast attend 1 CA.
*
* Pass in SA and obtain 60 % in total agg of SA and CA to clear
*
examMarksCalculatedMapGroupResultDF(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF).show(false)

* */
  def examMarksCalculatedMapGroupResultDF(incomingRecordDF:org.apache.spark.sql.DataFrame,referenceDF:org.apache.spark.sql.DataFrame)=examMarksCalculatedMapGroup(incomingRecordDF.where(s"examType = '${cumulativeAssessment}'"),referenceDF.where(s"examType = '${cumulativeAssessment}'")).as("ca")
    .join(examMarksCalculatedMapGroup(incomingRecordDF.where(s"examType = '${summativeAssessment}'"),referenceDF.where(s"examType = '${summativeAssessment}'")).as("sa"), "semId,studentId,subjectCode".split(",").toSeq).select(
    "semId,studentId,subjectCode".split(",").map(col) ++
      getCASACols( /* saMarksCalculatedMapGroupsDF */
        examMarksCalculatedMapGroup(incomingRecordDF.where(s"examType = '${summativeAssessment}'"),referenceDF.where(s"examType = '${cumulativeAssessment}'"))
        .columns.diff("semId,studentId,subjectCode".split(",")))  :_*
  ).groupByKey(x => (x.getAs[String]("semId"),x.getAs[String]("studentId"),x.getAs[String]("subjectCode")))
    .mapGroups((keys,rows) => {

      val rowsTmp= rows.toList
      val caMarksRecalculated=getPercentage(new java.math.BigDecimal(getMaxMarks("CA")),
        rowsTmp.head.getAs[java.math.BigDecimal]("ca_aggMarks"))
        .multiply(new java.math.BigDecimal(40.0 / 100),MathContext.DECIMAL128)

      val saMarksRecalculated=getPercentage(new java.math.BigDecimal(getMaxMarks("SA")),
        rowsTmp.head.getAs[java.math.BigDecimal]("sa_aggMarks"))
        .multiply(new java.math.BigDecimal(60.0 / 100),MathContext.DECIMAL128)

      val caPassMarkNew= getMarksForPercentage(new java.math.BigDecimal(40.0),new java.math.BigDecimal(45.0) )
      val saPassMarkNew= getMarksForPercentage(new java.math.BigDecimal(60.0),new java.math.BigDecimal(50.0) )

      val totalMarksCalculated=caMarksRecalculated.add(saMarksRecalculated).setScale(3,java.math.BigDecimal.ROUND_HALF_UP)

      val (semResult,semRemarks)=  totalMarksCalculated match {
        case value if rowsTmp.head.getAs[Int]("ca_examsAttended") == 0 =>
          ("FAIL","Did not appear in any CA")
        case value if rowsTmp.head.getAs[Int]("sa_examsAttended") == 0 =>
          ("FAIL","Did not appear in any SA")
        case value if !checkPass(saMarksRecalculated,saPassMarkNew) =>
          ("FAIL","Failed to clear SA")
        case value if !checkPass(value,new java.math.BigDecimal(60.0)) && checkPass(saMarksRecalculated,saPassMarkNew) =>
          ("FAIL","Failed in sem ,cleared SA")
        case value if checkPass(value,new java.math.BigDecimal(60.0))
          && rowsTmp.head.getAs[Int]("ca_examsAttended") >1  && checkPass(saMarksRecalculated,saPassMarkNew)
          &&  checkPass(caMarksRecalculated,caPassMarkNew) =>
          ("PASS","Keep pushing")
        case value if checkPass(value,new java.math.BigDecimal(60.0))
          && rowsTmp.head.getAs[Int]("ca_examsAttended") >1  && checkPass(saMarksRecalculated,saPassMarkNew)
          &&  !checkPass(caMarksRecalculated,caPassMarkNew) =>
          ("PASS","Keep pushing, try to clear CA")
        case value if checkPass(value,new java.math.BigDecimal(60.0))
          && rowsTmp.head.getAs[Int]("ca_examsAttended") ==1  && checkPass(saMarksRecalculated,saPassMarkNew)
          &&  !checkPass(caMarksRecalculated,caPassMarkNew) =>
          ("PASS","Keep pushing, try to attend and clear all CA")
        case value if checkPass(value,new java.math.BigDecimal(60.0))
          && rowsTmp.head.getAs[Int]("ca_examsAttended") ==1  && checkPass(saMarksRecalculated,saPassMarkNew)
          &&  checkPass(caMarksRecalculated,caPassMarkNew) =>
          ("PASS","Keep pushing, try to attend all CA")
      }
      Row(rowsTmp.head.getAs[String]("semId"),
        rowsTmp.head.getAs[String]("studentId"),
        rowsTmp.head.getAs[String]("subjectCode"),
        caMarksRecalculated,
        saMarksRecalculated,
        caPassMarkNew,
        saPassMarkNew,
        totalMarksCalculated,
        new java.math.BigDecimal(60.0),
        semResult,
        semRemarks,
        getGradeJava(new java.math.BigDecimal(100.0),totalMarksCalculated,"finalCalculation"),
        rowsTmp.head.getAs[Int]("sa_totalNumberOfExams"),
        rowsTmp.head.getAs[Int]("ca_totalNumberOfExams"),
        rowsTmp.head.getAs[Int]("sa_examsAttended"),
        rowsTmp.head.getAs[Int]("ca_examsAttended")
      )
    }) (RowEncoder(new StructType(Array(StructField("semId",StringType,true),
      StructField("studentId",StringType,true),
      StructField("subjectCode",StringType,true),
      StructField("caMarks",DecimalType(6,3),true),
      StructField("saMarks",DecimalType(6,3),true),
      StructField("caPassMarks",DecimalType(6,3),true),
      StructField("saPassMarks",DecimalType(6,3),true),
      StructField("totalMarks",DecimalType(6,3),true),
      StructField("passMarkForTotal",DecimalType(6,3),true),
      StructField("result",StringType,true),
      StructField("remarks",StringType,true),
      StructField("grade",StringType,true),
      StructField("saTotalNumberOfExams",IntegerType,true),
      StructField("caTotalNumberOfExams",IntegerType,true),
      StructField("saExamsAttended",IntegerType,true),
      StructField("caExamsAttended",IntegerType,true)
    )))).groupByKey(x=> (x.getAs[String](0),x.getAs[String](1)))
    .flatMapGroups((key,rows) => {
      val rowsTmp=rows.toList
      val totalRecord=rowsTmp.filter(_.getAs[String](2)=="subTotal").head

      rowsTmp.filter(_.getAs[String](2)!="subTotal") :+ Row(
        totalRecord.getAs[String](0),
        totalRecord.getAs[String](1),
        totalRecord.getAs[String](2),
        totalRecord.getAs[java.math.BigDecimal](3),
        totalRecord.getAs[java.math.BigDecimal](4),
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[java.math.BigDecimal](5)).reduce((x,y) => x.add(y)),
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[java.math.BigDecimal](6)).foldLeft(new java.math.BigDecimal(0.0))((x,y) => x.add(y)) ,
        totalRecord.getAs[java.math.BigDecimal](7),
      rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[java.math.BigDecimal](8)).reduceLeft((x,y) => x.add(y)) ,// .reduce((x,y) => x.add(y)),
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case true => "Re-Appear for failed subjects" case false => "All clear"} ,
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Failed")).size >1 && rowsTmp.map(_.getAs[String](10)).filter(_.contains("Did not")) .size >1 => "Concentrate on failed subjects next time, appear in all exams to increase the chances of clearing" case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Failed")).size >1 => "Concentrate on failed subjects next time" case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Did not")).size >1 => "Appear in all exams to increase the chances of clearing" case false => "Keep pushing"},
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case value if value == true && totalRecord.getAs[java.math.BigDecimal](7).compareTo(new java.math.BigDecimal(0)) ==0 => "F-" case true => "F" case false =>totalRecord.getAs[Int](11) },
        totalRecord.getAs[Int](12),
        totalRecord.getAs[Int](13),
        totalRecord.getAs[Int](14),
        totalRecord.getAs[Int](15)
      )
    })(RowEncoder(new StructType(Array(StructField("semId",StringType,true),
      StructField("studentId",StringType,true),
      StructField("subjectCode",StringType,true),
      StructField("caMarks",DecimalType(6,3),true),
      StructField("saMarks",DecimalType(6,3),true),
      StructField("caPassMarks",DecimalType(6,3),true),
      StructField("saPassMarks",DecimalType(6,3),true),
      StructField("totalMarks",DecimalType(6,3),true),
      StructField("passMarkForTotal",DecimalType(6,3),true),
      StructField("result",StringType,true),
      StructField("remarks",StringType,true),
      StructField("grade",StringType,true),
      StructField("saTotalNumberOfExams",IntegerType,true),
      StructField("caTotalNumberOfExams",IntegerType,true),
      StructField("saExamsAttended",IntegerType,true),
      StructField("caExamsAttended",IntegerType,true)
    ))))




}
