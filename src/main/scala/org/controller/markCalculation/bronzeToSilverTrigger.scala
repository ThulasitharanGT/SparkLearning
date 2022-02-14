package org.controller.markCalculation

import org.controller.markCalculation.marksCalculationConstant._
import org.controller.markCalculation.marksCalculationUtil._
import io.delta.tables._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// reads the latest record for studId, examId and subCode for that student and writes it into inter path,
// old records may come late to SA and CA. So we need to take only the latest and project it.
object bronzeToSilverTrigger {
  val spark=getSparkSession()
  val sc=spark.sparkContext
  sc.setLogLevel("ERROR")
  import spark.implicits._

  def main(args:Array[String]):Unit ={
    val inputMap= inputArrayToMap(args)
   /*


    readStreamFormat=delta
    pathArg="hdfs://localhost:8020/user/raptor/persist/marks/CA/"
    bronzeColsForSelect="examId,studentID,subjectCode,marks,revisionNumber,incomingTS"
    checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInter"


CA
spark-submit --master local --class org.controller.markCalculation.bronzeToSilverTrigger --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/CA/" bronzeColsForSelect="examId,studentID,subjectCode,marks,revisionNumber,incomingTS" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/CAInter" interPath="hdfs://localhost:8020/user/raptor/persist/marks/CATrigger/"

SA

spark-submit --master local --class org.controller.markCalculation.bronzeToSilverTrigger --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  readStreamFormat=delta path="hdfs://localhost:8020/user/raptor/persist/marks/SA/" bronzeColsForSelect="examId,studentID,subjectCode,marks,revisionNumber,incomingTS" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/checkpoint/SAInter" interPath="hdfs://localhost:8020/user/raptor/persist/marks/SATrigger/"


    */
    val readStreamDF=getReadStreamDF(spark,inputMap)

    val writeStreamDF=readStreamDF.writeStream.format("console").outputMode("append")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => foreachBatchFun(df,batchID,inputMap) )

    writeStreamDF.start

    spark.streams.awaitAnyTermination


    // we need to take the latest record for  the incoming keys and then send it to a location
// examId|studentID|subjectCode
  /*  DeltaTable.forPath(spark,"").as("alfa").merge(readStreamDF.as("delta")
      ,col("alfa.examId")===col("delta.examId")).whenNotMatched.insertExpr(Map(""->""))
*/




  }
 case class bronzeMarksData(examId:String,studentID:String,subjectCode:String,marks:scala.math.BigDecimal,revisionNumber:Int,incomingTS:java.sql.Timestamp)

/*

create database marks_db location 'hdfs://localhost:8020/user/raptor/persist/marks/' ;

create external table marks_db.CA_Table
(examId String,studentID String
,subjectCode String,marks Decimal(6,3)
,revisionNumber integer
,incomingTS timestamp)
stored as parquet
location
'hdfs://localhost:8020/user/raptor/persist/marks/CA/'
tblproperties('parquet.compression' = 'SNAPPY');
;


ALTER TABLE marks_db.CA_Table SET  tblproperties('parquet.compression' = 'SNAPPY');

*/
  def foreachBatchFun(df:org.apache.spark.sql.DataFrame,batchId:Long,inputMap:collection.mutable.Map[String,String]) ={
  /*  val dfSchema=df.schema
   val rowSchema=RowEncoder(dfSchema)
*/
    val readFromDFItems=df.select("examId,studentID,subjectCode".split(",").toSeq.map(col):_*).collect
      .map(x=>(x.getAs[String]("examId"),x.getAs[String]("studentID")
        ,x.getAs[String]("subjectCode")))

    val reqDF= spark.read.format("delta").load(inputMap("path"))
      .where(s"examId in ${getWhereCondition(readFromDFItems.map(_._1))} and studentID in ${getWhereCondition(readFromDFItems.map(_._2))} and subjectCode in ${getWhereCondition(readFromDFItems.map(_._3))}")
      .withColumn("rankCol",row_number.over(Window.partitionBy("examId","studentID","subjectCode").orderBy(desc("revisionNumber"))))
      .filter("rankCol =1").drop("rankCol")

       reqDF.write.format("delta").mode("append").save(inputMap("interPath"))


    // if you have tables in db , you can use this
  /*  df.groupByKey(x=> (x.getAs[String]("examId"),x.getAs[String]("studentID"),x.getAs[String]("subjectCode"))).
      mapGroups((x,y) => {
        val sqlString=s"select ${inputMap("bronzeColsForSelect")}  "
        y.toList.head
      })(rowSchema) */

  }
}
