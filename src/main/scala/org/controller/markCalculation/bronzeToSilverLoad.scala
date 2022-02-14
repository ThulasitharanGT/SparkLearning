package org.controller.markCalculation
// will read the intermediate path and update silver

import io.delta.tables.DeltaTable
import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._
import scala.util.{Failure, Success, Try}

/*

 silverPath=""


*/
object bronzeToSilverLoad {
  val spark=getSparkSession()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit ={
    val inputMap=inputArrayToMap(args)

    getReadStreamDF(spark,inputMap).writeStream.format("append")
      .outputMode("console").option("checkpointLocation","")
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long) =>
        forEachBatchFun(df,inputMap)).start

    spark.streams.awaitAnyTermination

   /*  .merge(.as("delta")
      ,col("alfa.studentID")===col("delta.StudentID")
          && col("alfa.examId")===col("delta.examId")
          && col("alfa.subjectCode")===col("delta.subjectCode")
      ).whenMatched(col("alfa.marks") === col("delta.marks"))
      .updateExpr(Map("delta.incomingTs"->"alfa.incomingTs"))
      .whenMatched
      .updateExpr(Map("delta.incomingTs"->"incomingTs","delta.marks"->"alfa.marks"))
      .whenNotMatched
      .insertAll
      .execute*/
  }

  val forEachBatchFun:(org.apache.spark.sql.DataFrame
    ,collection.mutable.Map[String,String])
    => Unit= (dfBatch:org.apache.spark.sql.DataFrame
              ,inputMap:collection.mutable.Map[String,String]) => Try{ getDeltaTable(spark,inputMap("silverPath"))} match {
    case Success(s)=> s.as ("alfa").merge (dfBatch.as ("delta")
  , col ("alfa.studentID") === col ("delta.StudentID")
  && col ("alfa.examId") === col ("delta.examId")
  && col ("alfa.subjectCode") === col ("delta.subjectCode")
  ).whenMatched (col ("alfa.marks") === col ("delta.marks") )
  .updateExpr (Map ("delta.incomingTs" -> "alfa.incomingTs") )
  .whenMatched
  .updateExpr (Map ("delta.incomingTs" -> "incomingTs", "delta.marks" -> "alfa.marks") )
  .whenNotMatched
  .insertAll
  .execute
    case Failure(f)=>
      dfBatch.write.mode("append").format("delta").save(inputMap("silverPath"))
  }



}
