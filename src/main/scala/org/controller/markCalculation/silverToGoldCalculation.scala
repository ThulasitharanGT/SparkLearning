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


 //  read from gold trigger
/*

// hdfs://localhost:8020/user/raptor/stream/checkpoint/SAInterSilver

// hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInterSilver

// reads calculates total for examId and stud id level and calculates pass fail percentage for subject level and total level

SA:
 spark-submit --class org.controller.markCalculation.silverToGoldCalculation --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --num-executors 2 --executor-memory 512m --driver-memory 512m --driver-cores 2 --master local /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  maxMarks=100 examType=SA checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAInterSilver" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/" readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/SA_SilverToTriggerInput/"  silverPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Silver/" examTypePath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/" goldPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Gold/" goldToDiamondTrigger="hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/"

CA:
 spark-submit --class org.controller.markCalculation.silverToGoldCalculation --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --num-executors 2 --executor-memory 512m --driver-memory 512m --driver-cores 2 --master local /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  maxMarks=100 examType=CA checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInterSilver" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/" readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/CA_SilverToTriggerInput/"  silverPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Silver/" examTypePath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/" goldPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Gold/" goldToDiamondTrigger="hdfs://localhost:8020/user/raptor/persist/marks/GoldToTriggerInput/"

 // gold will read and compute semester wise marks

 */


    getReadStreamDF(spark,inputMap).writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)=>
      forEachBatchFun(getLatestRecordsForKeys(df,inputMap),batchId,inputMap))
      .start

    spark.streams.awaitAnyTermination

  }

  val getLatestRecordsForKeys:(org.apache.spark.sql.DataFrame,scala.collection.mutable.Map[String,String])=>
    org.apache.spark.sql.DataFrame =  (df:org.apache.spark.sql.DataFrame,inputMap:scala.collection.mutable.Map[String,String]) => {

    val inputKeysForCalculation=getWhereConditionArrayTuple(df).distinct

    println(s"inputKeysForCalculation ${inputKeysForCalculation.deep}")

    val dfTmp=spark.read.format("delta").load(inputMap("silverPath"))
      .filter(s"examId in ${getWhereCondition(inputKeysForCalculation.map(_._1))} and studentID in ${getWhereCondition(inputKeysForCalculation.map(_._2))}")
      .select( col("examId"),col("studentID"),col("subjectCode"),col("marks"))


    dfTmp

  }

  def getWhereConditionArrayTuple(df:org.apache.spark.sql.DataFrame)=df.collect.map(x=>(x.getAs[String]("examId"),x.getAs[String]("studentID")))
 //  val getGradeUdf=udf(getGradeJavaNew(_:String,_:Float,_:String):String)

 // val getGradeUdf=udf(getGradeJava(_:scala.math.BigDecimal,_:java.math.BigDecimal,_:String):String)

  val getGradeUdf=udf(getGradeJavaUpdated(_:java.math.BigDecimal,_:java.math.BigDecimal,_:String):String)
  val getPassMarksUDF=udf(getPassMarkPercentage(_:String):Int)
  val array_filter_containsUDF = udf(array_filter_contains[String](_:Seq[String],_:String):Seq[String])

  def forEachBatchFun(df:org.apache.spark.sql.DataFrame,batchID:Long,inputMap:collection.mutable.Map[String,String])={
    /*
    val simpleDateFormat=new java.text.SimpleDateFormat("yyyy-MM-dd")

    Seq(("2019-2020","e001","FN")).toDF("assessmentYear,examId,examTime".split(",").toSeq:_*).write.format("delta").mode("append").save("hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/")
    Seq(("2019-2020","e002","FN")).toDF("assessmentYear,examId,examTime".split(",").toSeq:_*).write.format("delta").mode("append").save("hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/")
    Seq(("2019-2020","ex001","FN")).toDF("assessmentYear,examId,examTime".split(",").toSeq:_*).write.format("delta").mode("append").save("hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/")


// examId,subId,date  ==> mapping, Ref column

     assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/"

     Seq(("e001","CA"),("e002","CA"),("ex001","SA")).toDF("examId,examType".split(",").toSeq:_*).write.format("delta").mode("append").save("hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/")

      */
   val examAndAssessmentDetailsDF= spark.read.format("delta").load(inputMap("assessmentPath"))
   val assessmentAndAssessmentTypeDF= spark.read.format("delta").load(inputMap("examTypePath"))



    // total exams taken must come , max marks must come ,
    // then check CA and SA stats, semester result

   val tmpJoinDF=examAndAssessmentDetailsDF.as("assessmentData")
     .join(df.as("AllSubData"),Seq("examId"))
     .join(assessmentAndAssessmentTypeDF.as("allExam"),Seq("examId"))
     .select("examId",
       "AllSubData.studentId","AllSubData.subjectCode"
       ,"assessmentData.assessmentYear","AllSubData.marks"
       ,"allExam.examType")


    import spark.implicits._
/*  Following map groups because it reduces cpu over head

// calculating using joins agg  and union
    val calcDFTemp=tmpJoinDF.withColumn("passPercentage",getPassMarksUDF(col("examType")))
      .withColumn("maxMarks", when(col("examType") === lit(summativeAssessment), lit(100.0) ).otherwise(lit(60.0)))
      .withColumn("passMark",(col("maxMarks") / lit(100.0)) * col("passPercentage"))
      .withColumn("result",
        when(col("marks") >= col("passMark"),lit("pass")).otherwise("fail"))
      .withColumn("grade",getGradeUdf($"maxMarks",col("marks"),col("examType"))).union(
      tmpJoinDF.groupBy("examId|studentId|assessmentYear |examType".split("\\|").map(_.trim).map(col).toSeq:_*)
        .agg(count("*").as("totalSub"),sum("marks").as("marks"))
        .withColumn("subjectCode",lit("subTotal"))
        .withColumn("maxMarks", when($"examType"===lit(summativeAssessment), lit(100.0))
          .otherwise(lit(60.0)) * col("totalSub")).drop("totalSub")
        .withColumn("passPercentage",getPassMarksUDF(col("examType")))
        .withColumn("passMark", ($"maxMarks" / lit(100.0)) * col("passPercentage" ))
        .withColumn("result",lit("YetTo"))
        .withColumn("grade",getGradeUdf($"maxMarks",col("marks"),col("examType")))
        .select(
          "examId|studentId|subjectCode|assessmentYear|marks  |examType|passPercentage|maxMarks|passMark|result|grade"
            .split('|').map(_.trim)
          .toSeq.map(col):_*)  )


    val calcDF=calcDFTemp.as("proper").join(
      calcDFTemp.withColumn("keyColumn",concat(lower(col("result")),lit("~"),col("subjectCode")))
        .groupBy("examId|studentId|assessmentYear  |examType".split('|')
          .map(_.trim).map(col):_*)
        .agg(collect_list(col("keyColumn")).as("keyColList"))
        .withColumn("result",
          when(array_contains(col("keyColList"),lit("fail"))
            ,lit("FAIL")).otherwise(lit("PASS")))
        .withColumn("commentTmp",
          array_filter_containsUDF(col("keyColList"),lit("fail"))
        ).withColumn("comment",regexp_replace(concat_ws(", ",col("commentTmp"))
        ,lit("~"),lit("iled in "))).as("agg"),
    "examId|studentId|assessmentYear|examType".split("\\|").map(_.trim).toSeq)
      .select(col("examId"),col("studentId"),col("assessmentYear")
        ,col("examType"),col("proper.subjectCode"),col("proper.marks")
        ,col("proper.passPercentage")
        ,col("proper.maxMarks")
        ,col("proper.passMark")
      , when(col("subjectCode") === lit("subTotal"),col("agg.result"))
          .otherwise(col("proper.result")).as("result")
        ,col("proper.grade"),when(col("subjectCode")===lit("subTotal"),lit(""))
          .otherwise(col("agg.comment")).as("comment") )

    calcDF.withColumn("calcDF",lit("calcDF")).show(false)
*/
   /* "|examId|studentId|assessmentYear|examType|subjectCode|marks  |passPercentage|maxMarks|passMark|result|grade|keyColList  ".split("\\|")
      .filter(_.trim.size >0).map(_.trim).map((_,1)).ensuring(_.size >4)

    "|examId|studentId|assessmentYear|examType|subjectCode|marks | result||passPercentage|maxMarks|passMark|result|grade|keyColList  ".split("\\|")
      .filter(_.trim.size >0).map(_.trim).map((_,1)).foldLeft[scala.collection.mutable.Map[String,Int]](scala.collection.mutable.Map[String,Int]())(
      (tempResult,incoming)=> tempResult.get(incoming._1) match {
      case Some(x) =>
        tempResult.put(incoming._1,x+incoming._2)
        tempResult
      case None =>
        tempResult.put(incoming._1,incoming._2)
        tempResult
      }
    )

*/


// examID to semID mapping

    // calculating using sum by map groups
    val calcMapGroupsDF=tmpJoinDF.groupByKey(x=>(x.getAs[String]("studentId"),x.getAs[String]("examId"),
      x.getAs[String]("assessmentYear"),
      x.getAs[String]("examType"))).flatMapGroups((x,y)=>{

      var total=scala.math.BigDecimal(0.0)
      val listY=y.toList.map(z=> {total+= getBigDecimalFromRow(z,"marks") /*z.getAs[scala.math.BigDecimal]("marks")*/;z})
      println(s"total ${total}")
      println(s"listY ${listY}")

      val passMarkPercentage=getPassMarkPercentage(x._4)
      val maxMarks=getMaxMarks(x._4)
      val passMarkCalculated= Math.round((maxMarks/100.0) * passMarkPercentage)

      val finalistWithoutFinalResult=listY.map(x=> Row( getBigDecimalFromRow(x,"marks")
      ,x.getAs[String]("studentId")
      ,x.getAs[String]("subjectCode")
      ,x.getAs[String]("assessmentYear")
      ,x.getAs[String]("examId"),
        x.getAs[String]("examType"),
        getBigDecimalFromRow(x,"marks") match
        { case value if value.toLong >= passMarkCalculated => "pass"
        case value if value.toLong < passMarkCalculated => "fail" }
      ,passMarkPercentage
        ,maxMarks
      ,passMarkCalculated
      ) )

      println(s"finalistWithoutFinalResult ${finalistWithoutFinalResult}")

      finalistWithoutFinalResult.map(elem=>
        Row(elem.getString(4) // examId
          ,elem.getString(1) // studentId
          ,elem.getString(2) // subjectCode
          ,elem.getString(3) // assessmentYear
          ,elem.getAs[scala.math.BigDecimal](0)   //  marks
        ,  elem.getString(5) // examType
      , getGrade(scala.math.BigDecimal(getMaxMarks(x._4)),elem.getAs[scala.math.BigDecimal](0),x._4)
          ,  elem.getString(6) // result
          ,elem.getInt(7) // passMarkPercentage
          ,elem.getInt(8) // maxMarks
          ,elem.getLong(9) // passMarkCalculated
          , "" //
          )):+ Row(  //studentId|examId|assessmentYear|examType
        x._2,x._1,"subTotal",x._3,total,x._4,
        getGrade( scala.math.BigDecimal(Math.ceil(finalistWithoutFinalResult.size * getMaxMarks(x._4))),total ,x._4)
       , finalistWithoutFinalResult.map(_.getAs[String](5)).contains("fail")
        match {case value if value ==true => "FAIL" case false =>"PASS"},
        getPassMarkPercentage(x._4),
        getMaxMarks(x._4) * finalistWithoutFinalResult.size,
        Math.ceil((100.0/ (getMaxMarks(x._4)* finalistWithoutFinalResult.size))* getPassMarkPercentage(x._4) ).toLong,
        finalistWithoutFinalResult.map(x => (x.getAs[String](2) //("subjectCode")
          , x.getAs[String](6))).filter(_._2 == "fail") match
        {case value if value.size > 0  => s"Failed in ${value.map(_._1).mkString(",")}" case _ => "" })
    })  (RowEncoder(tmpJoinDF.schema.add(StructField("grade",StringType,true))
      .add(StructField("result",StringType,true))
      .add(StructField("passMarkPercentage",IntegerType,true))
      .add(StructField("maxMarks",IntegerType,true))
      .add(StructField("passMarkCalculated",LongType,true))
      .add(StructField("comment",StringType,true))))

    /*
    (RowEncoder(new StructType(Array(StructField("examId",StringType,true)
    ,StructField("studentID",StringType,true)
    ,StructField("subjectId",StringType,true)
    ,StructField("assessmentYear",StringType,true)
    ,StructField("marks",DecimalType(6,3),true)
      ,StructField("examType",StringType,true)
      ,StructField("grade",StringType,true)
      ,StructField("result",StringType,true)
      ,StructField("passMarkPercentage",IntegerType,true)
      ,StructField("maxMarks",IntegerType,true)
      ,StructField("passMarkCalculated",LongType,true)
      ,StructField("comment",StringType,true)))))
*/


      /*(RowEncoder(tmpJoinDF.schema match
    {case value =>
        value // .add(StructField("total",DecimalType(6,3),true))
        .add(StructField("result",StringType,true))
        .add(StructField("grade",StringType,true))
       .add(StructField("comment",StringType,true))

      /*.add(StructField("finalResult",StringType,true))*/}
    )*/


   // calcMapGroupsDF.withColumn("calcMapGroupsDF",lit("calcMapGroupsDF")).show(false)

    Try{getDeltaTable(spark,inputMap("goldPath"))} match {
      case Success(s)=>
        s.as("alpha").merge(calcMapGroupsDF.as("delta"),
          col("alpha.examId")===col("delta.examId")
            && col("alpha.studentId")===col("delta.studentId")
            && col("alpha.subjectCode")===col("delta.subjectCode")
            && col("alpha.assessmentYear")===col("delta.assessmentYear")
            && col("alpha.examType")===col("delta.examType")  ).
        whenMatched.updateExpr(Map("grade"-> "delta.grade" ,"result"-> "delta.result"
          ,"passMarkPercentage"-> "delta.passMarkPercentage" ,"maxMarks"-> "delta.maxMarks"
          ,"passMarkCalculated"-> "delta.passMarkCalculated" ,"comment"-> "delta.comment" ) )
          .whenNotMatched.insertAll.execute

        saveDF(calcMapGroupsDF.write.mode("append").format("delta"),inputMap("goldToDiamondTrigger"))

      /*

      |grade|result|passMarkPercentage|maxMarks|passMarkCalculated|comment

       examId|studentId|subjectCode|assessmentYear|marks  |examType|grade|result|passMarkPercentage|maxMarks|passMarkCalculated|commentexamId|studentId|subjectCode|assessmentYear|marks  |examType|grade|result|passMarkPercentage|maxMarks|passMarkCalculated|commentexamId|studentId|subjectCode|assessmentYear|marks  |examType|grade|result|passMarkPercentage|maxMarks|passMarkCalculated|commentexamId|studentId|subjectCode|assessmentYear|marks  |examType|grade|result|passMarkPercentage|maxMarks|passMarkCalculated|comment
       */

      case Failure(f)=>
        println(s"No delta table present")
        inputMap.put("writeMode","append")
        inputMap.put("writeFormat","delta")
        inputMap.put("writePath",inputMap("goldPath"))
        persistDF(calcMapGroupsDF,inputMap)

        saveDF(calcMapGroupsDF.write.mode("append").format("delta"),inputMap("goldToDiamondTrigger"))
    }

  }

}
