package org.controller.markCalculation

import org.apache.spark.sql.types._
object marksCalculationConstant {

  val readStreamFormat="readStreamFormat"
  val writeStreamFormat="writeStreamFormat"

  val deltaStreamFormat="delta"
  val kafkaStreamFormat="kafka"
  val pathArg="path"
  val kafkaBootstrapServerArg="kafkaBootstrapServer"
  val kafkaSubscribe="kafkaSubscribe"
  val kafkaAssign="kafkaAssign"
  val kafkaSubscribeAssignDecider="kafkaSubscribeAssignDecider"
  val kafkaTopic="kafkaTopic"

  val deltaMergeOverwriteDecider="deltaMergeOverwriteDecider"
  val deltaMerge="deltaMerge"
  val deltaOverwrite="deltaOverwrite"
  val deltaMergeAndOverwrite="deltaMergeAndOverwrite"


  val checkpointLocation="checkpointLocation"
  val summativeAssessment="SA"
  val cumulativeAssessment="CA"
  val wrapperSchema=new StructType(Array(StructField("messageType",StringType,true),StructField("actualMessage",StringType,true),StructField("receivingTimeStamp",StringType,true)))
  val innerMarksSchema=new StructType(Array(StructField("examId",StringType,true),
    StructField("studentID",StringType,true)
    ,StructField("subjectCode",StringType,true),
    StructField("marks",DecimalType(6,3),true)
  ,StructField("revisionNumber",IntegerType,true)))


}
