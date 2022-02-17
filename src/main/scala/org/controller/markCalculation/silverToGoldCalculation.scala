package org.controller.markCalculation
// calculate aggregate for marks,
// reads from CA/SA op and then calculates the marks for the stud,exam id
// and assessment year(examID to assessment year mapping to be done with SCD 2 in a new job).
// and updates gold aggregates to CA gold and SA Gold, (examid,studentId ) level

import scala.util.{Try,Success,Failure}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._

object silverToGoldCalculation {

  val spark=org.apache.spark.sql.SparkSession.builder.getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit={
    val inputMap=scala.collection.mutable.Map[String,String]()
    for(arg<- args)
      Try{arg.split("=",2)} match
        {
        case Success(s) => inputMap.put(s(0),s(1))
        case Failure(f) => {}
      }

//    readStreamFormat
 //   deltaStreamFormat
  //  pathArg

    getReadStreamDF(spark,inputMap).writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)=>{})
      .start


    spark.streams.awaitAnyTermination

  }

  def forEachBatchFun(df:org.apache.spark.sql.DataFrame,batchID:Long,inputMap:collection.mutable.Map[String,String])={
    /*
    val simpleDateFormat=new java.text.SimpleDateFormat("yyyy-MM-dd")
    Seq(("2019-2020","e001",new java.sql.Date(simpleDateFormat.parse("2020-01-02").getTime),"FN")).toDF("assessmentYear,examId,examDate,examTime".split(",").toSeq:_*).write.format("delta").mode("append").save("hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/")

     assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/"
      */
   val examAndAssessmentDetailsDF= spark.read.format("delta").load(inputMap("assessmentPath"))

    val examIdAndStudentId=df.collect.map(x=>(x.getAs[String]("examId"),x.getAs[String]("studentId")))

    val dfWithExamIdAndSubjectIds=spark.read.format("delta").load(inputMap("path")).filter(s"examId in (${getWhereCondition(examIdAndStudentId.map(_._1))}) and studentId in(${getWhereCondition(examIdAndStudentId.map(_._2))})")

   val tmpJoinDF=examAndAssessmentDetailsDF.as("assessmentData")
     .join(dfWithExamIdAndSubjectIds.as("AllSubData"),Seq("examId"))
     .select("examId","AllSubData.studentId","AllSubData.subjectCode","assessmentData.assessmentYear","AllSubData.marks")

    import spark.implicits._
    val passMarkPercentage=getPassMarkPercentage(inputMap)
    val maxMarks=inputMap("maxMarks").toInt
    val passMarkCalculated= (maxMarks/100.0) * passMarkPercentage

    val calcDF=tmpJoinDF.withColumn("result",
      when(col("marks")>= lit(passMarkCalculated),lit("pass")).otherwise("fail"))
      .withColumn("totalMarks",sum(col("marks")).over(
        org.apache.spark.sql.expressions.Window.partitionBy(col("studentId")
        , $"assessmentYear",col("examId"))))


    // calculating sum by map groups
    val calcMapGroupsDF=tmpJoinDF.groupByKey(x=>(x.getAs[String]("studentId"),x.getAs[String]("examId"),
      x.getAs[String]("assessmentYear"))).flatMapGroups((x,y)=>{
      var total=0
      val listY=y.toList.map(z=> {total+= z.getAs[Int]("marks");z})
     // val totalMarks=listY.map(z=> {total+= z.getAs[Int]("marks");z})
      val totalResult= ""
      // if this is not successful, try accumulator
      // marks, studentId,assessment year, examId
      val finalistWithoutFinalResult=listY.map(x=> Row(x.getAs[Int]("marks")
      ,x.getAs[String]("studentId")
      ,x.getAs[String]("assessmentYear")
      ,x.getAs[String]("examId")
      ,total,
      x.getAs[Int]("marks") match {case value if value >= passMarkCalculated => "pass" case value if value < passMarkCalculated => "fail" }))

      finalistWithoutFinalResult.map(x=>
        Row(x.getString(0)
          ,x.getString(1)
          ,x.getString(2)
          ,x.getString(3)
          ,x.getInt(4),
          x.getString(5),
          finalistWithoutFinalResult.map(_.getString(5)).filter(_.contains("fail")).size match {case value if value >= 1 => "FAIL" case 0 => "PASS"}))

    })(RowEncoder(tmpJoinDF.schema match {case value => value.add(StructField("total",IntegerType,true)).add(StructField("result",IntegerType,true)).add(StructField("finalResult",StringType,true))})) // add 2 columns

// totally pass or not
    calcDF.groupByKey(x=>(x.getAs[String]("studentId"),x.getAs[String]("examId"),
      x.getAs[String]("assessmentYear"))).flatMapGroups((x,y)=>{
      val listRows=y.toList
      val totalResult= listRows.map(_.getAs[String]("result")).filter(_.contains("fail")).size match {case value if value >= 1 => "FAIL" case 0 => "PASS"}
      listRows.map(x => Row(x.getAs[String]("studentId"),x.getAs[String]("examId"),
        x.getAs[String]("assessmentYear"),x.getAs[String]("result")
        ,x.getAs[String]("totalMarks"),totalResult))
    })(RowEncoder(calcDF.schema.add(StructField("finalResult",StringType,true))))


///////////////////////////////// do scd 1 and write it to final     table.


  }

}
