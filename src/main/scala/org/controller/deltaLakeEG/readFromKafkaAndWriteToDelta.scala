package org.controller.deltaLakeEG

import org.constants.projectConstants._
import org.util.SparkOpener
import org.util.readWriteUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object readFromKafkaAndWriteToDelta extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit ={
    val inputMap=  collection.mutable.Map[String,String]()
    for(arg <- args)
    {
      val keyPart=arg.split("=",2)(0)
      val valPart=arg.split("=",2)(1)
      inputMap.put(keyPart,valPart)
    }
   val inputSchema= StructType(Array(StructField("strValue",StringType,true),StructField("intValue",StringType,true),StructField("dateValue",StringType,true)))
    inputMap.put(fileFormatArg,kafkaFormat)
    inputMap.put(kafkaBootStrapServersArg,inputMap("bootstrapServers"))
    inputMap.put(kafkaValueDeserializerArg,kafkaStringSerializer)
    inputMap.put(kafkaKeyDeserializerArg,kafkaStringSerializer)
    inputMap.put(startingOffsetsArg,inputMap(startingOffsetsArg))
    inputMap.put(subscribeArg,inputMap("topic"))

     val readStreamDF=readStreamFunction(spark,inputMap).select(from_json(col("value").cast(StringType),inputSchema).as("finalColsArray")).selectExpr("finalColsArray.*")
    readStreamDF.printSchema
    inputMap.put(fileFormatArg,deltaFormat)
    inputMap.put(checkPointLocationArg,inputMap("checkpointLocation"))
    inputMap.put(outputModeArg,"append")
    inputMap.put(deltaMergeSchemaClause,"false")
    inputMap.put(deltaOverWriteSchemaClause,"false")
    inputMap.put(pathArg,inputMap("deltaOutputPath"))
    writeStreamAsDelta(spark,inputMap,readStreamDF).start

    spark.streams.awaitAnyTermination

// spark-submit --class org.controller.deltaLakeEG.readFromKafkaAndWriteToDelta --packages io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --driver-memory 512m --driver-cores 1 --num-executors 1 --executor-memory 512m --executor-cores 1  --master local[*] --conf spark.dynamicAllocation.enabled=false /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServers=localhost:9092,localhost:9093,localhost:9094 topic=deltaCheckTopic checkpointLocation=hdfs://localhost:8020/user/raptor/streams/deltaWriteStream/ deltaOutputPath=hdfs://localhost:8020/user/raptor/persist/deltaPersistPath/ startingOffsets=latest
  }

}
