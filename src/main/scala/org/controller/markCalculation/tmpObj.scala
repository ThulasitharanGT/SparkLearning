package org.controller.markCalculation

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

  def getSchema(schemaStr:String)={
    val schemaSplitArray=schemaStr.split(";")
    var fieldsArray:Seq[org.apache.spark.sql.types.StructField]=Seq.empty

  }

  def getDataType(dType:String):org.apache.spark.sql.types.DataType=dType.toLowerCase match {
    case value if value == stringType => org.apache.spark.sql.types.StringType
    case value if value.contains(decimalType) => org.apache.spark.sql.types.DecimalType
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

  spark-submit --class org.controller.markCalculation.tmpObj --num-executors 2 --executor-cores 2 --executor-memory 512m --driver-cores 2 --driver-memory 512m --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  bootstrapServers=localhost:8081,localhost:8082,localhost:8083 topic=tmpTopic startingOffset=latest numRows=999999 truncate=false checkpointLocation="hdfs://localhost:8020/user/raptor/streams/tmpStream"

   */
  spark.read.format("kafka")
    .option("kafka.bootstrap.servers",inputMap("bootstrapServers"))
    .option("subscribe",inputMap("topic"))
    .option("startingOffsets",inputMap("startingOffset"))
    .load.withColumn("value",org.apache.spark.sql.functions.col("value").cast(org.apache.spark.sql.types.StringType)).withColumn("valueExtract",org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col(""),getSchema(inputMap("schemaString")))).writeStream.format("console")
    .outputMode("append")
    .option("truncate",inputMap("truncate"))
    .option("numRows",inputMap("numRows"))
    .option("checkpointLocation",inputMap("checkpointLocation"))
    .start

    spark.streams.awaitAnyTermination

}
}
