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

//   readStreamFormat
 //   deltaStreamFormat
  //  pathArg
    // c
 //  read from gold trigger
/*

// hdfs://localhost:8020/user/raptor/stream/checkpoint/SAInterSilver

// hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInterSilver


SA:
 spark-submit --class org.controller.markCalculation.silverToGoldCalculation --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --num-executors 2 --executor-memory 512m --driver-memory 512m --driver-cores 2 --master local /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  maxMarks=100 examType=SA checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAInterSilver" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/" readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/SA_SilverToTriggerInput/"  silverPath="hdfs://localhost:8020/user/raptor/persist/marks/SA_Silver/" examTypePath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/"

CA:
 spark-submit --class org.controller.markCalculation.silverToGoldCalculation --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 --num-executors 2 --executor-memory 512m --driver-memory 512m --driver-cores 2 --master local /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  maxMarks=100 examType=CA checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInterSilver" assessmentPath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndAssessmentYearInfo/" readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/CA_SilverToTriggerInput/"  silverPath="hdfs://localhost:8020/user/raptor/persist/marks/CA_Silver/" examTypePath="hdfs://localhost:8020/user/raptor/persist/marks/examIdAndTypeInfo/"

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

    df.withColumn("df1",lit("df1")).show(false)

    val inputKeysForCalculation=getWhereConditionArrayTuple(df).distinct

    println(s"inputKeysForCalculation ${inputKeysForCalculation.deep}")

    val dfTmp=spark.read.format("delta").load(inputMap("silverPath"))
      .filter(s"examId in ${getWhereCondition(inputKeysForCalculation.map(_._1))} and studentID in ${getWhereCondition(inputKeysForCalculation.map(_._2))}")
      .select( col("examId"),col("studentID"),col("subjectCode"),col("marks"))

    dfTmp.withColumn("dfTmp",lit("dfTmp")).show(false)

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
     .select("examId","AllSubData.studentId","AllSubData.subjectCode","assessmentData.assessmentYear","AllSubData.marks","allExam.examType")


    import spark.implicits._
    val passMarkPercentage=getPassMarkPercentage(inputMap)
    val maxMarks=inputMap("maxMarks").toInt
    val passMarkCalculated= Math.round((maxMarks/100.0) * passMarkPercentage)

    // """|examId|studentId|subjectCode|assessmentYear|marks |result|grade""".split('|').map(_.trim).filter(_.size >0).map (x=> s"""col("${x}")""")

    tmpJoinDF.withColumn("passPercentage",getPassMarksUDF(col("examType")))
      .withColumn("maxMarks", when(col("examType") === lit(summativeAssessment), lit(100.0) ).otherwise(lit(60.0)))
      .withColumn("passMark",(col("maxMarks") / lit(100.0)) * col("passPercentage"))
      .withColumn("result",
        when(col("marks") >= col("passMark"),lit("pass")).otherwise("fail"))
      .withColumn("grade",getGradeUdf($"maxMarks",col("marks"),col("examType")))
      .show(false)

    /* val calcDF=tmpJoinDF.withColumn("result",
      when(col("marks") >= lit(passMarkCalculated),lit("pass")).otherwise("fail"))
      .withColumn("grade",getGradeUdf(lit(new java.math.BigDecimal(inputMap("maxMarks").toInt)),col("marks"),lit(inputMap("examType"))))
      .union(tmpJoinDF
      .withColumn("countCol",sum(lit(1))
      .over(org.apache.spark.sql.expressions.Window.partitionBy(col("examId"),
        $"studentId",$"assessmentYear")))
      .groupBy(col("examId")
      ,col("studentId"),$"assessmentYear",col("countCol"))
      .agg(sum("marks").as("marks"))
    .withColumn("subjectCode",lit("subTotal"))
        .withColumn("grade",getGradeUdf(lit(new java.math.BigDecimal(inputMap("maxMarks").toInt))/* col("countCol")*/ ,col("marks"),lit(inputMap("examType"))))
      .withColumn("result",when(col("marks")>= lit(scala.math.BigDecimal(passMarkCalculated))
        ,lit("PASS")).otherwise(lit("FAIL"))).drop("countCol")
        .select("""|examId|studentId|subjectCode|assessmentYear|marks |result|grade""".split('|')
          .map(_.trim).filter(_.size >0).map(col).toSeq:_* ) ///.map (x=> s"""col("${x}")"""))
    ) */
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
        .select("examId|studentId|subjectCode|assessmentYear|marks  |examType|passPercentage|maxMarks|passMark|result|grade".split('|').map(_.trim)
          .toSeq.map(col):_*)
    )

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
      ,lit("~"),lit("iled in "))).show(false)


    val calcDF=calcDFTemp.as("proper").join(
      calcDFTemp.withColumn("keyColumn",concat(lit("~"),col("result"),col("subjectCode")))
        .groupBy("examId|studentId|subjectCode|assessmentYear  |examType".split('|')
      .map(_.trim).map(col):_*)
        .agg(collect_list(col("keyColumn")).as("keyColList"))
        .withColumn("result",
        when(array_contains(lower(col("keyColumn")),lit("fail"))
          ,lit("FAIL")).otherwise(lit("PASS")))
        .withColumn("commentTmp",
          array_filter_containsUDF(col("keyColumn"),lit("fail"))
        ).withColumn("comment",regexp_replace(concat_ws(", ",col("commentTmp")),lit("~"),lit("iled in "))).as("agg"),
    "examId|studentId|subjectCode|assessmentYear|examType".split("\\|").map(_.trim).toSeq)


    //  [examId#1550, studentId#1049, subjectCode#1050, assessmentYear#1549, marks#1051, examType#1943, passPercentage#2258, maxMarks#2266, passMark#2275, result#2285, UDF(cast(maxMarks#2266 as decimal(38,18)), marks#1051, examType#1943) AS grade#2296]
// examID to semID mapping
    calcDF.withColumn("calcDF",lit("calcDF")).show(false)

    // calculating sum by map groups
    val calcMapGroupsDF=tmpJoinDF.groupByKey(x=>(x.getAs[String]("studentId"),x.getAs[String]("examId"),
      x.getAs[String]("assessmentYear"))).flatMapGroups((x,y)=>{
      var total=scala.math.BigDecimal(0.0)
      val listY=y.toList.map(z=> {total+= getBigDecimalFromRow(z,"marks") /*z.getAs[scala.math.BigDecimal]("marks")*/;z})
      println(s"total ${total}")
      println(s"listY ${listY}")

      // val totalMarks=listY.map(z=> {total+= z.getAs[Int]("marks");z})
      // if this is not successful, try accumulator
      // marks, studentId,assessment year, examId

      val finalistWithoutFinalResult=listY.map(x=> Row( getBigDecimalFromRow(x,"marks")//x.getAs[scala.math.BigDecimal]("marks")
      ,x.getAs[String]("studentId")
      ,x.getAs[String]("subjectCode")
      ,x.getAs[String]("assessmentYear")
      ,x.getAs[String]("examId")
,      /* x.getAs[scala.math.BigDecimal]("marks") */
        getBigDecimalFromRow(x,"marks") match
        { case value if value.toLong >= passMarkCalculated => "pass"
        case value if value.toLong < passMarkCalculated => "fail" }))
      println(s"finalistWithoutFinalResult ${finalistWithoutFinalResult}")
      /*
      finalistWithoutFinalResult.map(x=>
        Row(x.getString(4)
          ,x.getString(1)
          ,x.getString(2)
          ,x.getString(3)
          ,x.getAs[scala.math.BigDecimal](0)   // ,x.getDecimal(0),
          ,x.getAs[scala.math.BigDecimal](5),  //  x.getDecimal(5) ,
          x.getString(6),
            finalistWithoutFinalResult.map(_.getString(6)).filter(_.contains("fail")).size match {case value if value >= 1 => "FAIL" case 0 => "PASS"}))
*/
      finalistWithoutFinalResult.map(x=>
        Row(x.getString(4)
          ,x.getString(1)
          ,x.getString(2)
          ,x.getString(3)
          ,x.getAs[scala.math.BigDecimal](0)   // ,x.getDecimal(0),
        ,  x.getString(5)
      , getGrade(x.getAs[scala.math.BigDecimal](0),scala.math.BigDecimal(inputMap("maxMarks").toInt),inputMap("examType"))
      , "" )):+ Row(
        x._2,x._1,"subTotal",x._3,total,
        finalistWithoutFinalResult.map(_.getAs[String](5)).contains("fail")
        match {case value if value ==true => "FAIL" case false =>"PASS"},
      getGrade( total, scala.math.BigDecimal(finalistWithoutFinalResult.size * inputMap("maxMarks").toInt),inputMap("examType"))
        ,finalistWithoutFinalResult.map(x => (x.getAs[String](2) //("subjectCode")
          , x.getAs[String](5))).filter(_._2 == "fail") match
        {case value if value.size > 0  => s"Failed in ${value.map(_._1).mkString(",")}" case _ => "" })
    })(RowEncoder(new StructType(Array(StructField("examId",StringType,true)
    ,StructField("studentID",StringType,true)
    ,StructField("subjectId",StringType,true)
    ,StructField("assessmentYear",StringType,true)
    ,StructField("marks",DecimalType(6,3),true)
    ,StructField("result",StringType,true)
    ,StructField("grade",StringType,true)
      ,StructField("comment",StringType,true)))))

      /*(RowEncoder(tmpJoinDF.schema match
    {case value =>
        value // .add(StructField("total",DecimalType(6,3),true))
        .add(StructField("result",StringType,true))
        .add(StructField("grade",StringType,true))
       .add(StructField("comment",StringType,true))

      /*.add(StructField("finalResult",StringType,true))*/}
    )*/


// examId",
    // "AllSubData.studentId"
    // ,"AllSubData.subjectCode",
    // "assessmentData.assessmentYear",
    // "AllSubData.marks


    calcMapGroupsDF.withColumn("calcMapGroupsDF",lit("calcMapGroupsDF")).show(false)

    /*

// totally pass or not
    calcDF.groupByKey(x=>(x.getAs[String]("studentId"),x.getAs[String]("examId"),
      x.getAs[String]("assessmentYear"))).flatMapGroups((x,y)=>{
      val listRows=y.toList
      val totalResult= listRows.map(_.getAs[String]("result")).filter(_.contains("fail")).size match {case value if value >= 1 => "FAIL" case 0 => "PASS"}
      listRows.map(x => Row(x.getAs[String]("studentId"),x.getAs[String]("examId"),
        x.getAs[String]("assessmentYear"),x.getAs[String]("result")
        ,x.getAs[String]("totalMarks"),totalResult))
    })(RowEncoder(calcDF.schema.add(StructField("finalResult",StringType,true))))
*/


///////////////////////////////// do scd 1 and write it to final  table.


  }

}
