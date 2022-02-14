package org.controller.markCalculation

import org.controller.markCalculation.marksCalculationConstant._
import org.controller.markCalculation.marksCalculationUtil._

object tmpObj {


def main(args:Array[String]):Unit={
  val spark=org.apache.spark.sql.SparkSession.builder.enableHiveSupport.getOrCreate

  val inputMap=inputArrayToMap(args)
  val stringType="string"
  val decimalType="decimal"
  val integerType ="integer"
  val intType="int"
  val floatType ="float"
  val timestampType ="timestamp"
  val dateType ="date"
  val strType="str"

  def getSchema(schemaStr:String)={
    val schemaSplitArray=schemaStr.split(";").map(_.split(" ") match {case value if value.size ==2 => (value(0),value(1))})
    var fieldsSeq:Seq[org.apache.spark.sql.types.StructField]=Seq.empty
    for( schemaInfo <- schemaSplitArray)
      fieldsSeq = fieldsSeq :+ org.apache.spark.sql.types.StructField(schemaInfo._1,getDataType(schemaInfo._2),true)
   new org.apache.spark.sql.types.StructType(fieldsSeq.toArray)
  }

  def getDataType(dType:String):org.apache.spark.sql.types.DataType=dType.toLowerCase match {
    case value if value == stringType || value == strType => org.apache.spark.sql.types.StringType
    case value if value.contains(decimalType) =>
      val precisionAndScaleSplit= value.split(",").map(_ match {case value if value.contains("(") => (value.split('(').last.toInt,"s") case value if value.contains(")") => (value.split(')').head.toInt,"p")})
      org.apache.spark.sql.types.DecimalType(precisionAndScaleSplit.filter(_._2 =="s").head._1,precisionAndScaleSplit.filter(_._2 =="p").head._1)
    case value if value == integerType || value == intType  => org.apache.spark.sql.types.IntegerType
    case value if value == floatType => org.apache.spark.sql.types.FloatType
    case value if value == timestampType => org.apache.spark.sql.types.TimestampType
    case value if value == dateType =>org.apache.spark.sql.types.DateType
  }


  /*
  bootstrapServers=localhost:8081,localhost:8082,localhost:8083
  topic=tmpTopic
  startingOffset=latest
  numRows=999999
  truncate=false
  checkpointLocation="hdfs://localhost:8020/user/raptor/streams/tmpStream"
  schemaString="messageType string;actualMessage str;receivingTimeStamp decimal(10,4)"

  spark-submit --class org.controller.markCalculation.tmpObj --num-executors 2 --executor-cores 2 --executor-memory 512m --driver-cores 2 --driver-memory 512m --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  bootstrapServers=localhost:8081,localhost:8082,localhost:8083 topic=tmpTopic startingOffset=latest numRows=999999 truncate=false checkpointLocation="hdfs://localhost:8020/user/raptor/streams/tmpStream" schemaString="messageType string;actualMessage str;receivingTimeStamp decimal(10,4)"

   */
    /*
     spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers",inputMap("bootstrapServers"))
    .option("subscribe",inputMap("topic"))
    .option("startingOffsets",inputMap("startingOffset"))
    .load
    .withColumn("value",org.apache.spark.sql.functions.col("value").cast(org.apache.spark.sql.types.StringType))
    .withColumn("valueExtract",org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value"),getSchema(inputMap("schemaString"))))
    .select(org.apache.spark.sql.functions.col("*"),org.apache.spark.sql.functions.col("valueExtract.*"))
    .drop(org.apache.spark.sql.functions.col("valueExtract"))
    .writeStream.format("console")
    .outputMode("append")
    .option("truncate",inputMap("truncate"))
    .option("numRows",inputMap("numRows"))
    .option("checkpointLocation",inputMap("checkpointLocation"))
    .start
*/
     inputMap.put(readStreamFormat,kafkaStreamFormat)
     inputMap.put(kafkaBootstrapServerArg,inputMap("bootstrapServers"))
     inputMap.put(kafkaSubscribeAssignDecider,kafkaSubscribe)
     inputMap.put(kafkaSubscribe,inputMap("topic"))
     inputMap.put(kafkaStartingOffsetsArg,inputMap("startingOffset"))

     getReadStreamDF(spark,inputMap)
    .withColumn("value",org.apache.spark.sql.functions.col("value").cast(org.apache.spark.sql.types.StringType))
    .withColumn("valueExtract",org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value"),getSchema(inputMap("schemaString"))))
    .select(org.apache.spark.sql.functions.col("*"),org.apache.spark.sql.functions.col("valueExtract.*"))
    .drop(org.apache.spark.sql.functions.col("valueExtract"))
    .writeStream.format("console")
    .outputMode("append")
       .foreachBatch((df:org.apache.spark.sql.DataFrame,batchId:Long)
       =>{forEachBatchFun(df,batchId)})
//    .option("truncate",inputMap("truncate"))
//   .option("numRows",inputMap("numRows"))
    .option("checkpointLocation",inputMap("checkpointLocation"))
    .start


    spark.streams.awaitAnyTermination

}

  def forEachBatchFun(df:org.apache.spark.sql.DataFrame,batchId:Long)=df.withColumn("batchID",org.apache.spark.sql.functions.lit(batchId)).show(false)

}
