package org.controller.markCalculation
// sem level

import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import java.math.{BigInteger, MathContext}
import org.apache.spark.sql.expressions.Window
/*

 spark-submit --num-executors 2 --executor-cores 2 --driver-memory 512m --executor-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --class org.controller.markCalculation.diamondCalculation /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/" goldCAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Gold/" goldSAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Gold/" semIdExamIDAssessmentYear="hdfs://localhost:8020/user/raptor/persist/marks/assessmentYearInfo_scd2/" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAGoldToDiamondCalc" examIdToExamType="hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDAndExamType_scd2/" diamondPath="hdfs://localhost:8020/user/raptor/persist/marks/diamondCalculatedAndPartitioned/" semIdAndExamIdAndSubCode=hdfs://localhost:8020/user/raptor/persist/marks/semIDAndExamIDAndSubCode_scd2/

res0.map(_.split("=",2) match { case value => inputMap.put(value(0),value(1) match {case value if value.startsWith("\"") && value.endsWith("\"") => value.drop(1).dropRight(1) case value if value.startsWith("\"") => value.drop(1) case value if value.endsWith("\"") => value.dropRight(1) case value => value })})

val df= spark.read.format("delta").load("hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/").filter(" (examId in ('ex001','e001') and studentId ='stu001')  or (studentId ='stu002')")

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import java.math.MathContext
import org.apache.spark.sql.expressions.Window

saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF).filter(" (examId in ('ex001','e001') and studentId ='stu001')  or (studentId ='stu002')")

val semAndExamIdDF=examIDsOfSemIdDF


* */

object diamondCalculation {
  val spark = getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  spark.udf.register("maxMarksUDF", getMaxMarksFinal(_: String))

  val maxMarksUDF = udf(getMaxMarksFinal(_: String): Int)
  val getGradeUDF = udf(getGradeJava(_: java.math.BigDecimal, _: java.math.BigDecimal, _: String): String)
  val arrayFilterEqualsUDF = udf(array_filter_equals[String](_: Seq[String], _: String): Seq[String])
  val arrayFilterContainsUDF = udf(array_filter_contains[String](_: Seq[String], _: String): Seq[String])
  val getCommentMkString = udf(mkString(_:Seq[String]):String)
  val attendanceCommentManipulator=udf(attendanceCommentCreator(_:String,_:String):String)


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
    //  spark.table("exam_id_sem_id_exam_type").withColumn("finalRef",lit("finalRef")).show(false)

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

    // semIdDF.withColumn("semIdDF", lit("semIdDF")).show(false)

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

    //   examIDsOfSemIdDF.withColumn("examIDsOfSemIdDF", lit("examIDsOfSemIdDF")).show(false)

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

    //  saRecordsForIncomingKeysDF.withColumn("saRecordsForIncomingKeysDF", lit("saRecordsForIncomingKeysDF")).show(false)

    val caRecordsForIncomingKeysDF = caGoldInfo
      .where(s"examId in ${
        getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._1).toArray)
      } and studentId in ${
        getWhereCondition(
          caExamIdAndStudentIdInfo.map(_._2).toArray)
      }").withColumn("examType", lit(cumulativeAssessment))




    //   caRecordsForIncomingKeysDF.withColumn("caRecordsForIncomingKeysDF", lit("caRecordsForIncomingKeysDF")).show(false)
    // atleast 1 ca must be attended, doesn't matter fail or pass.

    ///// else just read gold SA and CA and join with incoming studId
    // and ExamId and filter subTotal and calc. But this gives inconsistent results.
    // SEM logic 60 % from SA (1 Exam usually) , 40 % from (CA's , usually 2 to 3 exams taken in average)

    // logic 1  read separately , join method

    val finalResultDF =getFinalResultDFJoin(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF.withColumnRenamed("number_of_assessments","num_of_assessments"))


    getMarksCalculatedJoinAdvancedRow(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF).show(false)
    getMarksCalculatedJoinAdvanced(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF).show(false)
    finalResultDF.withColumn("finalResultDF",lit("finalResultDF")).show(false)
    examMarksCalculatedMapGroupResultDF(saRecordsForIncomingKeysDF.union(caRecordsForIncomingKeysDF),examIDsOfSemIdDF.withColumnRenamed("number_of_assessments","num_of_assessments")).withColumn("finalMapGroupsDF",lit("finalMapGroupsDF")).show(false)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    saveDF(finalResultDF.write.mode("overwrite").format("delta").partitionBy("semId", "studentId"), inputMap("diamondPath"))

    // 2nd method using mapgroups

  }
  def getMarksCalculatedJoinAdvanced(incomingRecords:org.apache.spark.sql.DataFrame
                                     ,semAndExamIdDF:org.apache.spark.sql.DataFrame)=incomingRecords.as("incoming").
    join(semAndExamIdDF.as("reference"),"examId,subjectCode,studentId,examType".split(",").toSeq
      ,"right").withColumn("newMaxMarks",maxMarksUDF(col("examType"))).
    withColumn("newMaxMarksCalculated",col("newMaxMarks")/col("number_of_assessments")).
    withColumn("newPassMarksCalculated", coalesce(col("passMarkPercentage") *
      (col("newMaxMarksCalculated") / lit(100.0) ),lit(0.0))
      .cast(DecimalType(6,3))).
    withColumn("marksPercentage",col("marks") * round((lit(100.0) /col("maxMarks")),3).cast(DecimalType(6,3))).
    withColumn("numberOfExamsAttendedPerSubject",sum(when(col("marks").isNull,lit(0)).otherwise(lit(1)))
      .over(Window.partitionBy("studentID","subjectCode","examType"))).withColumn("attendanceCommentTmp",
    when(coalesce(col("numberOfExamsAttendedPerSubject"),lit(0)) === col("number_of_assessments")
      ,lit("ALL")).otherwise(lit("NOT-ALL"))).
    withColumn("keyCombination", concat(col("examId"),lit("~"),col("subjectCode"),lit("~")
      ,coalesce(col("marks"),lit("")))).
    withColumn("attendanceCommentTmp2", split(col("keyCombination"),"~")).
    withColumn("attendanceCommentTmp2",expr("filter(attendanceCommentTmp2, x -> x != '')")).
    withColumn("commentTmp", when(size(col("attendanceCommentTmp2")) === lit("2")
      && col("attendanceCommentTmp") === lit("NOT-ALL"),
      concat(col("attendanceCommentTmp2")(1)
        ,lit(":"),col("attendanceCommentTmp2")(0))
    ).otherwise(lit(""))).withColumn("newMarksCalculated",
    ((col("newMaxMarksCalculated")/lit(100.0).cast(DecimalType(6,3)) ) * col("marksPercentage") )
      .cast(DecimalType(6,3))).withColumn("totalPerExamSubjectType",sum(coalesce(col("newMarksCalculated"),lit(0).cast(DecimalType(6,3))))
    .over(Window.partitionBy("studentId","subjectCode","examType"))).
    withColumn("totalPerExamType",sum(coalesce(col("newMarksCalculated"),lit(0).cast(DecimalType(6,3))))
      .over(Window.partitionBy("studentId","examType"))).
    drop("marks |grade|result|attendanceCommentTmp|keyCombination     |attendanceCommentTmp2".split("\\|").map(_.trim):_*).
    withColumn("maxMarksPerExamType",sum(col("newMaxMarksCalculated")).
      over(Window.partitionBy("semId","studentID","examType","examId"))).
    withColumn("maxMarksPerExamType",(col("maxMarksPerExamType")
      * col("number_of_assessments")).cast(DecimalType(6,3))).
    withColumn("newPassMarksCalculatedPerExamType", sum(col("newPassMarksCalculated"))
      .over(Window.partitionBy("semId","studentId","examType"))).
    withColumn("newPassMarksCalculatedPerExamType", max(col("newPassMarksCalculatedPerExamType"))
      .over(Window.partitionBy("semId,examType".split(",").map(col):_*))) // should have included deptID for studentId
  match {
    case interIncoming =>

      val calcIncoming=interIncoming.select("semId,studentID,subjectCode,examType,numberOfExamsAttendedPerSubject,number_of_assessments,newPassMarksCalculated,newMaxMarks,totalPerExamSubjectType,totalPerExamType,maxMarksPerExamType,newPassMarksCalculatedPerExamType".split(",").map(col) :_* ).
        withColumnRenamed("newMaxMarks","maxMarksCalculated").withColumnRenamed("newPassMarksCalculated","passMarksCalculated").
        withColumn("passMarksCalculated",max(col("passMarksCalculated")).
          over(Window.partitionBy("semId,studentID,subjectCode,examType".split(",").map(col):_*))).
        withColumn("rowNumber",row_number.over(Window.partitionBy("semId,studentID,subjectCode,examType,numberOfExamsAttendedPerSubject,number_of_assessments,passMarksCalculated,maxMarksCalculated,totalPerExamSubjectType,totalPerExamType,maxMarksPerExamType,newPassMarksCalculatedPerExamType".split(",")
          .map(col) :_*).orderBy(col("semId")))). // to take one record per multiple exam Id's
        filter("rowNumber =1").drop("rowNumber").as("a").join(interIncoming.filter("commentTmp != '' ").
        groupBy("semId","studentID","examType").agg(collect_list(col("commentTmp")).as("finalComments")).as("a").
        join(interIncoming.filter("commentTmp !=''").as("b"),Seq("semId","studentID","examType"),"right").
        select("semId |studentID|examType|finalComments|subjectCode|commentTmp".
          split("\\|").map(_.trim).map(col) :_*).as("b")
        ,Seq("semId","studentID","examType","subjectCode")
        ,"left").orderBy("semId","studentID","examType","subjectCode").withColumn("commentTmp",coalesce(col("commentTmp"),lit(""))).
        withColumn("finalComments",coalesce(concat_ws(",",col("finalComments")),lit("")))

      //   foldLeft(lit(1)===lit(1))((seqVar,tmpVar) => seqVar && col(s"a.${tmpVar}") === col(s"b.${tmpVar}"))

      val finalCalculated=calcIncoming.
        select("semId |studentID|examType|subjectCode|numberOfExamsAttendedPerSubject numberOfExamsAttended|number_of_assessments totalNumberOfAssessments|maxMarksCalculated maxMarks|passMarksCalculated|totalPerExamSubjectType marks|finalComments|commentTmp".split("\\|").map(_.trim.split(" ") match {case value => col(value.head).as(value.last)  } )
          :_*). //.withColumn("passMarksCalculated",sum("passMarksCalculated").over(Window.partitionBy("semId","studentId","subjectCode","examType"))).
        withColumn("passMarksCalculated", ( col("passMarksCalculated") * col("totalNumberOfAssessments")).cast(DecimalType(6,3))).
        withColumn("marks",col("marks").cast(DecimalType(6,3))).
        withColumn("maxMarks",col("maxMarks").cast(DecimalType(6,3))).
        select("semId |studentID|examType|subjectCode|numberOfExamsAttendedPerSubject numberOfExamsAttended|number_of_assessments totalNumberOfAssessments|passMarksCalculated|maxMarksPerExamType maxMarks|totalPerExamSubjectType marks|finalComments|commentTmp".split("\\|").map(_.trim.split(" ") match {case value => col(value.last).as(value.last)  }) :_*).
        union(
          calcIncoming.
            select("semId |studentID|examType|subjectCode|numberOfExamsAttendedPerSubject numberOfExamsAttended|number_of_assessments totalNumberOfAssessments|maxMarksPerExamType maxMarks|passMarksCalculated|totalPerExamSubjectType marks|finalComments|commentTmp".split("\\|").map(_.trim.split(" ") match {case value => col(value.head).as(value.last)  }) :_*).
            groupBy("semId |studentID|examType|finalComments".split("\\|").map(_.trim).map(col):_*).
            agg(sum("marks").cast(DecimalType(6,3)).as("marks")
              ,max("maxMarks").cast(DecimalType(6,3)).as("maxMarks")
              ,sum("numberOfExamsAttended").as("numberOfExamsAttended")
              ,sum("totalNumberOfAssessments").as("totalNumberOfAssessments")
              ,sum(col("passMarksCalculated") * col("totalNumberOfAssessments")).cast(DecimalType(6,3)).as("passMarksCalculated")).
            withColumn("subjectCode",lit("subTotal")).
            withColumn("commentTmp",lit(""))
            select("semId |studentID|examType|subjectCode|numberOfExamsAttendedPerSubject numberOfExamsAttended|number_of_assessments totalNumberOfAssessments|passMarksCalculated|maxMarksPerExamType maxMarks|totalPerExamSubjectType marks|finalComments|commentTmp".split("\\|").map(_.trim.split(" ") match {case value => col(value.last).as(value.last)  }) :_*)
        ).orderBy("semId |studentID|examType|subjectCode".split('|').map(_.trim).map(col):_*)


      val finalResults=finalCalculated.filter("examType='CA'").as("ca").join(finalCalculated.filter(col("examType")===lit("SA")).as("sa")
        ,Seq("semId","studentId","subjectCode")).withColumn("totalForSem",(col("sa.marks") + col("ca.marks")).cast(DecimalType(6,3)) ).
        withColumn("totalMaxForSem",(col("sa.maxMarks") + col("ca.maxMarks")).cast(DecimalType(6,3))).
        select("semId,studentId,subjectCode,sa.examType as sa_examType, ca.examType as ca_examType, sa.numberOfExamsAttended as sa_numberOfExamsAttended, ca.numberOfExamsAttended as ca_numberOfExamsAttended, sa.totalNumberOfAssessments as sa_totalNumberOfAssessments, ca.totalNumberOfAssessments as ca_totalNumberOfAssessments, sa.maxMarks as sa_maxMarks, ca.maxMarks as ca_maxMarks, sa.marks as sa_marks, ca.marks as ca_marks,ca.passMarksCalculated as ca_passMarksCalculated,sa.passMarksCalculated as sa_passMarksCalculated, sa.finalComments as sa_finalComments, ca.finalComments as ca_finalComments, sa.commentTmp as sa_commentTmp, ca.commentTmp as ca_commentTmp,totalForSem,totalMaxForSem".split(",").map(_.trim.split(" ").filter(_.size >0) match {case value => col(value.head).as(value.last)}) :_*).
        withColumn("resultComment",when(col("sa_marks") >= col("sa_passMarksCalculated")
          , when(col("totalForSem") >= lit(60.0).cast(DecimalType(6,3))
            ,when(col("ca_marks") >= col("ca_passMarksCalculated"),
              lit("PASS in SA and CA"))
              .otherwise(lit("PASS in SA and total, but fail in CA")))
            .otherwise(lit("Failed to achieve total pass"))
        ).otherwise(lit("FAIL in SA"))).
        withColumn("result",when(col("sa_marks") >= col("sa_passMarksCalculated")
          , when(col("totalForSem") >= lit(60.0).cast(DecimalType(6,3))
            ,lit("PASS"))
            .otherwise(lit("FAIL"))
        ).otherwise(lit("FAIL"))).
        withColumn("finalComments",
          when(trim(col("ca_finalComments")) =!= lit("") && trim(col("sa_finalComments")) =!= lit("")
            ,concat(col("ca_finalComments"),lit(","),col("sa_finalComments"))
          ).when(trim(col("ca_finalComments")) =!= lit("") , col("ca_finalComments"))
            .when(trim(col("sa_finalComments")) =!= lit("") , col("sa_finalComments")).
            otherwise(lit(""))).
        withColumn("attendanceCommentTotal",attendanceCommentManipulator(col("finalComments"),lit("total"))).
        withColumn("attendanceCommentSeparate",
          when(length(trim(col("sa_commentTmp"))) =!= lit(0) && length(trim(col("sa_commentTmp"))) =!= lit(0)
            ,concat(col("sa_commentTmp"),lit(","),col("ca_commentTmp")) ).
            when( length(trim(col("sa_commentTmp"))) =!= lit(0) , trim(col("sa_commentTmp")))
            .when(length(trim(col("ca_commentTmp"))) =!= lit(0) , trim(col("ca_commentTmp"))).otherwise(lit(""))).
        withColumn("attendanceCommentSeparate",
          attendanceCommentManipulator(col("attendanceCommentSeparate"),lit("separate"))).
        drop("finalComments","ca_finalComments","sa_finalComments","sa_commentTmp","ca_commentTmp","sa_examType","ca_examType").
        withColumn("resultManipulation",concat(col("result"),lit("~"),col("subjectCode")))


      finalResults.filter("subjectCode != 'subTotal'").groupBy(Seq("semId","studentId").map(col) :_*).
        agg(collect_list(col("resultManipulation")).as("resultManipulation")).join(finalResults.drop("resultManipulation")
        ,Seq("semId","studentId")).withColumn("resultTmp",expr("exists(resultManipulation, x -> x like '%FAIL%')")).
        withColumn("result",when(col("subjectCode")===lit("subTotal"),
          when(col("resultTmp") ===lit(true),lit("RE-APPEAR")).otherwise(lit("ALL-CLEAR"))
        ).otherwise(col("result")) ).
        withColumn("attendanceComment",when(col("subjectCode")===lit("subTotal"),
          col("attendanceCommentTotal")
        ).otherwise(col("attendanceCommentSeparate")) ).orderBy("semId,studentId,subjectCode".split(",").map(col) :_*).
        drop("resultManipulation,resultTmp,attendanceCommentTotal,attendanceCommentSeparate".split(","):_*).
        withColumn("grade",getGradeUDF(col("totalMaxForSem"),col("totalForSem"),lit("finalCalculation")))

  }



  def attendanceCommentCreator(comment:String,typeVal:String)=     comment.split(",").map(_.trim).filter(_.size > 0) match {
    case value =>
      typeVal match {
        case temp if temp=="total" =>
          value match {
            case value if value.size > 0 =>
              value.map(_.split(":")).map(x => (x.head, x.last)) match {
                case value =>
                  value.groupBy(_._2).foldRight("Did not attend ")((currentKey, commentString) => s"${commentString}\n ${currentKey._2.map(_._1).mkString(",")} subjectID's for examId ${currentKey._1}")
              }
            case value if value.size == 0 =>
              ""
          }
        case "separate" =>
          comment.split(",").map(_.trim).filter(_.size > 0) match {
            case value if value.size != 0 =>
              s"Did not attend ${value.flatMap(_.split(":")).toList.grouped(2).collect { case a :: b => b.headOption match {case Some(x) => x case None =>""} }.filterNot(_=="").mkString(",")}"
            // value.grouped(2).collect{case a if a.size ==2 => a(1)}.mkString(",")
            case value if value.size == 0 =>
              ""
            case _ =>
              throw new Exception("Not a valid attendance comment")
          }
      }
  }



  def f(arr:List[Int]):List[Int] = {
    def loop(source:List[Int],destination:List[Int]):List[Int]= source match {
      case _::odd::tail => loop(tail,destination :+ odd)
      case _ => destination
    }
    loop(arr,List())
  }


  def getBigDecimalFromInt(intValue:Int=0)= new java.math.BigDecimal(intValue)
  def getBigDecimalFromRow(row:org.apache.spark.sql.Row,columnName:String)= row.getAs[java.math.BigDecimal](columnName)

  import org.apache.spark.sql.catalyst.encoders.RowEncoder

  def getMarksCalculatedJoinAdvancedRow(incomingRecords:org.apache.spark.sql.DataFrame
                                        ,semAndExamIdDF:org.apache.spark.sql.DataFrame)=incomingRecords.as("incoming").
    join(semAndExamIdDF.as("reference"),"examId,subjectCode,studentId,examType".split(",").toSeq
      ,"right").groupByKey(x => (x.getAs[String]("semId")
    ,x.getAs[String]("studentId")
    ,x.getAs[String]("examType"))).
    flatMapGroups((key,iterator) => {

      println("key "+key)

      val maxMarksNew=key._3 match {case "CA" => 40 case "SA"=> 60 }
      val passPercentageNew=key._3 match {case "CA" => 45 case "SA"=> 50 }
      val list=iterator.toList

      println("maxMarksNew "+maxMarksNew)
      println("passPercentageNew "+passPercentageNew)
      println("list "+list)

      val newMaxMarks=new java.math.BigDecimal(maxMarksNew.toString,java.math.MathContext.DECIMAL128)
        .divide(
          new java.math.BigDecimal(list.head.getAs[Long]("number_of_assessments"))
          ,java.math.MathContext.DECIMAL128)

      val newMarksList=list.map(x =>
        Row(x.getAs[String]("semId"),
          x.getAs[String]("examId"),
          x.getAs[String]("subjectCode"),
          x.getAs[String]("studentId"),
          x.getAs[String]("examType"),
          x.getAs[String]("assessmentYear"),
          getBigDecimalFromRow(x,"marks"),
          x.getAs[String]("grade"),
          x.getAs[String]("result"),
          x.getAs[Int]("passMarkPercentage"),
          x.getAs[Int]("maxMarks"),
          x.getAs[Int]("passMarkCalculated"),
          x.getAs[String]("comment"),
          x.getAs[Long]("number_of_assessments")
          , newMaxMarks
          ,  getBigDecimalFromInt(x.getAs[Int]("maxMarks")) match {
            case value if value==null =>
              println("maxMarks null")
              getBigDecimalFromInt()
            case value if getBigDecimalFromInt().compareTo(value)==0=>
              println("maxMarks 0")
              getBigDecimalFromInt()
            case value =>
              println("maxMarks value")
              getBigDecimalFromRow(x,"marks").multiply(getBigDecimalFromInt(100)
                .divide(value,java.math.MathContext.DECIMAL128)
                ,java.math.MathContext.DECIMAL128)
          }  // percentageOfMarksScored
          , (getBigDecimalFromInt(x.getAs[Int]("maxMarks")),getBigDecimalFromRow(x,"marks")) match {
            case (maxMarks,marks) if maxMarks == null || marks==null =>
              println("newMarks null")
              getBigDecimalFromInt()
            case (maxMarks,marks) if maxMarks.compareTo(getBigDecimalFromInt()) == 0 || marks.compareTo(getBigDecimalFromInt()) == 0 =>
              println("newMarks 00")
              getBigDecimalFromInt()
            case (maxMarks,marks) =>
              println(s"newMarks value value ${maxMarks} ${marks}")
              newMaxMarks.divide(getBigDecimalFromInt(100),java.math.MathContext.DECIMAL128)
                .multiply(marks.multiply(getBigDecimalFromInt(100)
                  .divide(maxMarks,java.math.MathContext.DECIMAL128)
                  ,java.math.MathContext.DECIMAL128)
                  ,java.math.MathContext.DECIMAL128)
          }// newMarks
          , newMaxMarks.divide(getBigDecimalFromInt(100),java.math.MathContext.DECIMAL128).
            multiply(getBigDecimalFromInt(passPercentageNew),java.math.MathContext.DECIMAL128) // newPassMark
          ,list.filter(_.getAs[String]("subjectCode") == x.getAs[String]("subjectCode")).
            map( r => (r.getAs[Long]("number_of_assessments"),getBigDecimalFromRow(r,"marks")) )  match
          {case value /*if value.filter(_._2==null).size >0 */ =>value.filter(_._2!=null).size }   // num_of_assessments_attended
        )
      )

      /*
       RowEncoder(new StructType(Array(StructField("semId",StringType,true)
      ,StructField("examId",StringType,true)
      ,StructField("subjectCode",StringType,true)
      ,StructField("studentId",StringType,true)
      ,StructField("examType",StringType,true)
      ,StructField("assessmentYear",StringType,true)
      ,StructField("marks",DecimalType(6,3),true)
      ,StructField("grade",StringType,true)
      ,StructField("result",StringType,true)
      ,StructField("passMarkPercentage",IntegerType,true)
      ,StructField("maxMarks",IntegerType,true)
      ,StructField("passMarkCalculated",IntegerType,true)
      ,StructField("comment",StringType,true)
      ,StructField("number_of_assessments",LongType,true)
      ,StructField("newMaxMarks",DecimalType(6,3),true)
      ,StructField("percentageOfMarksScored",DecimalType(6,3),true) //15
      ,StructField("newMarks",DecimalType(6,3),true)
      ,StructField("newPassMark",DecimalType(6,3),true)
    ,StructField("num_of_assessments_attended",IntegerType,true)))
    )
    */

      val newResult=newMarksList.map(x =>
        Row(
          x.getAs[String](0), // semId
          x.getAs[String](1), //examId
          x.getAs[String](2), //subjectCode
          x.getAs[String](3), //studentId
          x.getAs[String](4),  //examType
          x.getAs[String](5), //assessmentYear
          x.getAs[java.math.BigDecimal](16), // new marks
          x.getAs[java.math.BigDecimal](17), // new pass marks
          x.getAs[java.math.BigDecimal](14), // new max marks
          x.getAs[Long](13), // num_of_assessments
          x.getAs[Int](18) , // num_of_assessments_attended
          x.getAs[String](5) match { // assessment year is checked for null
            case null => s"${x.getAs[String](1)}~${x.getAs[String](2)}" // examId~subCode
            case _ => ""
          }, // examId Not attended
          newMarksList.map(_.getAs[java.math.BigDecimal](14)).map( _ match {case null => getBigDecimalFromInt() case value =>  value}).
            foldLeft(getBigDecimalFromInt())((totalMarks,currentMarks) => totalMarks.add(currentMarks,java.math.MathContext.DECIMAL128)) // max Marks per examType
          , newMarksList.filter(_.getAs[String](2) == x.getAs[String](2)).map(_.getAs[java.math.BigDecimal](16)).
            foldRight(getBigDecimalFromInt())((currentMarks,totalMarks) => totalMarks.add(currentMarks,java.math.MathContext.DECIMAL128)) // subCode level total
          , newMarksList.filter(_.getAs[String](2) == x.getAs[String](2)).map(_.getAs[java.math.BigDecimal](17)).
            foldRight(getBigDecimalFromInt())((currentMarks,totalMarks) => totalMarks.add(currentMarks,java.math.MathContext.DECIMAL128)) // sub code level pass marks total
          , newMarksList.map(_.getAs[java.math.BigDecimal](17)).
            foldRight(getBigDecimalFromInt())((currentMarks,totalMarks) => totalMarks.add(currentMarks,java.math.MathContext.DECIMAL128)) // exam level pass marks total
          , newMarksList.map(_.getAs[java.math.BigDecimal](16)).map( _ match {case null => getBigDecimalFromInt() case value =>  value}).
            foldLeft(getBigDecimalFromInt())((totalMarks,currentMarks) => totalMarks.add(currentMarks,java.math.MathContext.DECIMAL128)) // examTypeLevel total
        )
      )
      /* new StructType(Array(StructField("semId",StringType,true)
        ,StructField("examId",StringType,true)
        ,StructField("subjectCode",StringType,true)
        ,StructField("studentId",StringType,true)
        ,StructField("examType",StringType,true)
        ,StructField("assessmentYear",StringType,true)
        ,StructField("marks",DecimalType(6,3),true)
        ,StructField("passMark",DecimalType(6,3),true)
        ,StructField("maxMarks",DecimalType(6,3),true)
        ,StructField("number_of_assessments",LongType,true)
        ,StructField("number_of_assessments_attended",IntegerType,true)
        ,StructField("comment_attendance",StringType,true)
        ,StructField("totalPerExamType",DecimalType(6,3),true)
        ,StructField("totalPerSubCodeLevel",DecimalType(6,3),true)
        ,StructField("totalPassMarkPerSubCodeLevel",DecimalType(6,3),true)
        ,StructField("totalPassMarkPerExamType",DecimalType(6,3),true)
         ,StructField("totalMarksPerExamType",DecimalType(6,3),true)

        */
      val subjectCodeList=newResult.map(_.getAs[String](2)).distinct

      subjectCodeList.map( subCode =>
        newResult.filter(_.getAs[String](2) == subCode) match {
          case value =>
            Row(key._1 , //semId
              key._2 , // studentId
              key._3 , // examType
              subCode,
              value.head.getAs[java.math.BigDecimal](13), // sub level total
              value.head.getAs[java.math.BigDecimal](14), // sub level pass mark
              getBigDecimalFromInt(maxMarksNew) , // max marks
              value.map(_ match { case x => (x.getAs[String](5),x.getAs[String](11))}).filter(_._1 == null).map(_._2).mkString(","), // combined exam level sub level comment
              value.head.getAs[java.math.BigDecimal](13).compareTo(value.head.getAs[java.math.BigDecimal](14)) match {
                case value if List(0,1).contains(value) => "pass"
                case -1 => "fail"
              }, // sub level result
              value.map(_.getAs[String](11)).filter(_.trim.size>0).flatMap(_.split("~")) match {case value if value.size% 2 ==0 && value.size !=0 => s"${value.grouped(2).map(_.toArray).foldLeft("")((commentStr,currentInfo) => commentStr.trim.size >0 match {case true => s"${commentStr},${currentInfo(0)}" case false => s"Did not attend ${currentInfo(0)}"  })}" case _ => ""} // attendanceComment
            )
        }
      )  match {
        case value =>
          println(s"value in ${value}")
          value :+ Row(key._1, //semId
            key._2, // studentId
            key._3, // examType
            "subTotal",
            newResult.head.getAs[java.math.BigDecimal](16), // exam type level total
            newResult.head.getAs[java.math.BigDecimal](15), // exam level passmark total
            newResult.head.getAs[java.math.BigDecimal](12), // total mark per exam Type
            value.filter(_.getAs[String](8) =="fail").map(_.getAs[String](3)).mkString(","), // failed subjects comment
            value.filter(_.getAs[String](8) =="fail").size match {case value if value >0 => "FAIL" case _ => "PASS"},
            newResult.map(_.getAs[String](11)).filter(_.trim.size >0).map(_.split("~")).groupBy(_(1)).map(x => s"Did not appear in ${x._2.head(0)} for ${x._1} ." ).mkString("Attendance report: \n","\n","")
          )
      }

    })(RowEncoder( new StructType(
      Array(
        StructField("semId",StringType,true),
        StructField("studentId",StringType,true),
        StructField("examType",StringType,true),
        StructField("subCode",StringType,true),
        StructField("marks",DecimalType(6,3),true),
        StructField("passMarks",DecimalType(6,3),true),
        StructField("maxMarks",DecimalType(6,3),true),
        StructField("comments",StringType,true),
        StructField("result",StringType,true),
        StructField("attendanceComments",StringType,true)
      ))))  // no idea new group by key is not recognizing column names from the row encoder
    .groupByKey(x => (x.getAs[String](0) // semId
      ,x.getAs[String](1) ) // studentId
    ).flatMapGroups((key,dataIterator)=>{

    val dataList=dataIterator.toList

    val totalRecord=dataList.filter(_.getAs[String](3)=="subTotal")
    val allOtherRecord=dataList.filter(_.getAs[String](3)!="subTotal")

    val semPassMark=new java.math.BigDecimal(60)
    println(s"dataList ${dataList}")

    val caList=allOtherRecord.filter(_.getAs[String](2)=="CA")
    val saList=allOtherRecord.filter(_.getAs[String](2)=="SA")

    val subList=allOtherRecord.map(_.getAs[String](3)).distinct

    val finalCalculationWithComments= subList.foldLeft(List.empty[Row]) ((rowList,incomingSub)=>
    {
      val caRow=caList.filter(_.getAs[String](3)==incomingSub).head
      val saRow=saList.filter(_.getAs[String](3)==incomingSub).head

      println(s"caRow ${caRow}")
      println(s"saRow ${saRow}")

      rowList :+ Row(key._1, //semId
        key._2,//studentId
        incomingSub , //subCode
        caRow.getAs[java.math.BigDecimal](4), // ca marks
        saRow.getAs[java.math.BigDecimal](4), // sa marks
        caRow.getAs[java.math.BigDecimal](5), // ca pass marks
        saRow.getAs[java.math.BigDecimal](5), // sa pass marks
        caRow.getAs[java.math.BigDecimal](4).add(saRow.getAs[java.math.BigDecimal](4),java.math.MathContext.DECIMAL128), // total marks
        Array(0, 1).contains(caRow.getAs[java.math.BigDecimal](4).add(saRow.getAs[java.math.BigDecimal](4),java.math.MathContext.DECIMAL128)
          .compareTo(semPassMark)) match {
          case true =>
            saRow.getAs[java.math.BigDecimal](4).compareTo(saRow.getAs[java.math.BigDecimal](5)) match {
              case compResult if List(0, 1).contains(compResult) =>
                caRow.getAs[java.math.BigDecimal](4).compareTo(caRow.getAs[java.math.BigDecimal](5)) match {
                  case compResult if List(0, 1).contains(compResult) => "Passed ,Cleared SA and CA"
                  case _ => "Passed ,Cleared to clear SA. But failed to clear CA"
                }
              case _ => "Failed to clear SA"
            }
          case false => "Failed to achieve pass mark in sem total"
        },
        (caRow.getAs[java.math.BigDecimal](4).add(saRow.getAs[java.math.BigDecimal](4),java.math.MathContext.DECIMAL128)
          .compareTo(semPassMark), saRow.getAs[java.math.BigDecimal](4).compareTo(saRow.getAs[java.math.BigDecimal](5))) match {
          case value if getSuccessCheck(value._1) && getSuccessCheck(value._2) => "PASS"
          case _ => "FAIL"
        }
        ,saRow.getAs[String](9),
        caRow.getAs[String](9)
        ,""
      )
    }) match {
      case value =>
        println(s"value in ${value}")
        val caRow=totalRecord.filter(_.getAs[String](2)=="CA").head
        val saRow=totalRecord.filter(_ match { case value => value.getAs[String](2)=="SA" }).head

        value :+ Row(
          value.head.getAs[String](0), // semId
          key._2, // studentId
          "subTotal", // subCode
          caRow.getAs[java.math.BigDecimal](4), //caMarks
          saRow.getAs[java.math.BigDecimal](4), //saMarks
          caRow.getAs[java.math.BigDecimal](5), //ca passMarks
          saRow.getAs[java.math.BigDecimal](5), //sa passMarks
          caRow.getAs[java.math.BigDecimal](4).add(saRow.getAs[java.math.BigDecimal](4),java.math.MathContext.DECIMAL128), // totalMarks
          Array(0, 1).contains(caRow.getAs[java.math.BigDecimal](4).add(saRow.getAs[java.math.BigDecimal](4),java.math.MathContext.DECIMAL128)
            .compareTo(semPassMark.multiply(getBigDecimalFromInt(subList.size),java.math.MathContext.DECIMAL128))) match {
            case true =>
              saRow.getAs[java.math.BigDecimal](4).compareTo(saRow.getAs[java.math.BigDecimal](5)) match {
                case compResult if List(0, 1).contains(compResult) =>
                  caRow.getAs[java.math.BigDecimal](4).compareTo(caRow.getAs[java.math.BigDecimal](5)) match {
                    case compResult if List(0, 1).contains(compResult)  && value.map(_.getAs[String](9)=="FAIL").size ==0=>
                      "Passed ,Cleared SA and CA"
                    case compResult if List(0, 1).contains(compResult) =>
                      s"Failed ,Cleared SA and CA in total, but failed in ${value.filter(_.getAs[String](9)=="FAIL").map(_.getAs[String](2)).mkString(",")}"
                    case _ => "Passed ,Cleared to clear SA. But failed to clear CA"
                  }
                case _ => "Failed to clear SA"
              }
            case false => "Failed to achieve pass mark in sem total"
          },
          (caRow.getAs[java.math.BigDecimal](4).add(saRow.getAs[java.math.BigDecimal](4),java.math.MathContext.DECIMAL128)
            .compareTo(semPassMark.multiply(getBigDecimalFromInt(subList.size),java.math.MathContext.DECIMAL128))
            , saRow.getAs[java.math.BigDecimal](4).compareTo(saRow.getAs[java.math.BigDecimal](6))) match {
            case resultVal if getSuccessCheck(resultVal._1) && getSuccessCheck(resultVal._2)
              && value.map(_.getAs[String](9)=="FAIL").size ==0  => "PASS"
            case _ => "FAIL"
          }
          ,saRow.getAs[String](9).split("\n").map(_.trim).filter(_.size >0) match {case value if value.size ==1 => "" case value => saRow.getAs[String](9)},
          caRow.getAs[String](9).split("\n").map(_.trim).filter(_.size >0) match {case value if value.size ==1 => "" case value => caRow.getAs[String](9)}
          ,value.map(_ match {case x => (x.getAs[String](9),x.getAs[String](2))}).filter(_._2=="FAIL").distinct match {
            case sizeCheck if sizeCheck.size ==0 => ""
            case  failureVal => failureVal.map(_._2).mkString("Failed in ",","," subjects")
          }

        )
    }

    /*new StructType(Array(StructField("semId",StringType,true)
  ,StructField("studentId",StringType,true)
    ,StructField("subCode",StringType,true)
    ,StructField("ca_marks",DecimalType(6,3),true)
    ,StructField("sa_marks",DecimalType(6,3),true)
    ,StructField("ca_passMarks",DecimalType(6,3),true)
    ,StructField("sa_passMarks",DecimalType(6,3),true)
    ,StructField("semTotal",DecimalType(6,3),true)
    ,StructField("resultComment",StringType,true)
    ,StructField("result",StringType,true)
    ,StructField("sa_attendanceComment",StringType,true)
    ,StructField("ca_attendanceComment",StringType,true)
    ,StructField("failComments",StringType,true)
  )))*/
    finalCalculationWithComments.map(x =>
      Row(
        x.getAs[String](0),
        x.getAs[String](1),
        x.getAs[String](2),
        x.getAs[java.math.BigDecimal](3),
        x.getAs[java.math.BigDecimal](4),
        x.getAs[java.math.BigDecimal](5),
        x.getAs[java.math.BigDecimal](6),
        x.getAs[java.math.BigDecimal](7),
        x.getAs[String](8),
        (x.getAs[String](10).trim.size,x.getAs[String](11).trim.size) match {case (0,0) => s"" case (0,a) if a>0=> s"""CA: \n ${x.getAs[String](11)}""" case (a,0) if a>0 => s"""SA: \n ${x.getAs[String](10)}""" case (a,b) =>s"""SA: \n ${x.getAs[String](10)} \nCA: \n ${x.getAs[String](11)}"""}
        ,x.getAs[String](12)))

  }
  )(RowEncoder(new StructType(Array(StructField("semId",StringType,true)
    ,StructField("studentId",StringType,true)
    ,StructField("subCode",StringType,true)
    ,StructField("ca_marks",DecimalType(6,3),true)
    ,StructField("sa_marks",DecimalType(6,3),true)
    ,StructField("ca_passMarks",DecimalType(6,3),true)
    ,StructField("sa_passMarks",DecimalType(6,3),true)
    ,StructField("semTotal",DecimalType(6,3),true)
    ,StructField("result",StringType,true)
    ,StructField("attendanceComment",StringType,true)
    ,StructField("failComments",StringType,true)
  )))).withColumn("grade",getGradeUDF(lit(100.0),col("semTotal"),lit("finalCalculation")))



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

    println(s"rowList ${rowList.head.schema}")
    // val examIds=rowList.map(_.getAs[String]("examId")).distinct
    val numberOfExamIds = rowList.head.getAs[Long]("num_of_assessments")
    val maxMarkPerExamId = getMaxMarks(rowList.head.getAs[String]("examType")) / numberOfExamIds

    val numberOfExamsAttended = rowList.filter(x => x.getAs[String]("assessmentYear") != "NA"
      && x.getAs[String]("grade") != "NA"
      && x.getAs[String]("result") != "NA"
      && x.getAs[String]("comment") != "NA"
      && List(1, -1).contains(x.getAs[java.math.BigDecimal]("marks").compareTo(new java.math.BigDecimal(0.000)))
      && x.getAs[Int]("passMarkPercentage") != 0
      && x.getAs[Int]("maxMarks") != 0
    ).map(_.getAs[String]("examId")).distinct

    val examsNotAttended = rowList.filter(x => x.getAs[String]("assessmentYear") == "NA"
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
              examPerSubject._2.head.getLong(17), //total Number of exams
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
    , StructField("totalNumberOfExams", LongType, true),
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
        rowsTmp.head.getAs[Long](10),
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
      , StructField("totalNumberOfExams", LongType, true),
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
      println(s"inside after join rowsTmp ${rowsTmp} ")

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
        getGradeJava(new java.math.BigDecimal(100.0),totalMarksCalculated,"finalCalculation"),
        rowsTmp.head.getAs[Long]("sa_totalNumberOfExams"),
        rowsTmp.head.getAs[Long]("ca_totalNumberOfExams"),
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
      StructField("saTotalNumberOfExams",LongType,true),
      StructField("caTotalNumberOfExams",LongType,true),
      StructField("saExamsAttended",IntegerType,true),
      StructField("caExamsAttended",IntegerType,true)
    )))).groupByKey(x=> (x.getAs[String](0),x.getAs[String](1)))
    .flatMapGroups((key,rows) => {
      val rowsTmp=rows.toList
      val totalRecord=rowsTmp.filter(_.getAs[String](2)=="subTotal").head

      println(s"inside after join second rowsTmp ${rowsTmp} ")

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
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Failed")).size >=1 && rowsTmp.map(_.getAs[String](10)).filter(_.contains("Did not")) .size >0 => "Concentrate on failed subjects next time, appear in all exams to increase the chances of clearing" case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Failed")).size >0 => "Concentrate on failed subjects next time" case true if rowsTmp.map(_.getAs[String](10)).filter(_.contains("Did not")).size >0 => "Appear in all exams to increase the chances of clearing" case false => "Keep pushing"},
        rowsTmp.filter(_.getAs[String](2)!="subTotal").map(_.getAs[String](9)).contains("FAIL") match {case value if value == true && totalRecord.getAs[java.math.BigDecimal](7).compareTo(new java.math.BigDecimal(0)) ==0 => "F-" case true => "F" case false =>totalRecord.getAs[Int](11) },
        totalRecord.getAs[Long](12),
        totalRecord.getAs[Long](13),
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
      StructField("saTotalNumberOfExams",LongType,true),
      StructField("caTotalNumberOfExams",LongType,true),
      StructField("saExamsAttended",IntegerType,true),
      StructField("caExamsAttended",IntegerType,true)
    ))))




}
