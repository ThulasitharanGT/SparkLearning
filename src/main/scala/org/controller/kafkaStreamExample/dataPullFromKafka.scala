package org.controller.kafkaStreamExample

import org.constants.projectConstants
import org.util.{SparkOpener,readWriteUtil}

object dataPullFromKafka extends SparkOpener{
  val spark=SparkSessionLoc("spark session For pushing data to kafka")
  def main(args:Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg<-args)
    {
      val keyPart=arg.split("=",2)(0)
      val valPart=arg.split("=",2)(1)
      inputMap.put(keyPart,valPart)
    }
    val bootStrapServer=inputMap("bootStrapServer")
    val keyDeserializer=inputMap("keyDeserializer")
    val valueDeserializer=inputMap("valueDeserializer")
    val topicName=inputMap("topicName")
    val checkPointLocationWrite=inputMap("checkPointLocationWrite")
    val checkPointLocationRead=inputMap("checkPointLocationRead")//"file:///home/raptor/checkpointStream/checkpoint1" checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpointCarSensorDataFromStream/
    val outputPath=inputMap("outputPath") // outputPath=hdfs://localhost/user/raptor/kafka/stream/temp/consoleCarSensorDataFromStream/
    inputMap.put(projectConstants.kafkaBootStrapServersArg,bootStrapServer)
    inputMap.put(projectConstants.kafkaValueDeserializerArg,valueDeserializer)
    inputMap.put(projectConstants.kafkaKeyDeserializerArg,keyDeserializer)
    inputMap.put(projectConstants.subscribeArg,topicName)// topic name
    inputMap.put(projectConstants.startingOffsetsArg,"earliest")
    inputMap.put(projectConstants.fileFormatArg,projectConstants.kafkaFormat)
    inputMap.put(projectConstants.checkPointLocationArg,checkPointLocationRead)
    val readStreamDF = readWriteUtil.readStreamFunction(spark,inputMap).selectExpr("*","split(value,'~') as valueSplitted").drop("value").selectExpr("offset", "topic", "timestamp", "cast(valueSplitted[0] as string)as value", "cast(valueSplitted[1] as string) as date", "cast(valueSplitted[2] as string) as raceTrack", "cast(valueSplitted[3] as string) as runType", "timestampType", "partition", "cast (key  as string) as key")
    inputMap.put(projectConstants.fileFormatArg,projectConstants.deltaFormat)
    inputMap.put(projectConstants.checkPointLocationArg,checkPointLocationWrite)
    inputMap.put(projectConstants.pathArg,outputPath)
    inputMap.put(projectConstants.outputModeArg,"append")
    val writeStreamDF = readWriteUtil.writeStreamFunction(spark,inputMap,readStreamDF).partitionBy("date","topic","partition","key").start()
    //readStreamDF.writeStream.outputMode("append").format(projectConstants.deltaFormat).option("checkpointLocation",checkPointLocation).option("path",outputPath).partitionBy("date","topic","partition","key").start()
    writeStreamDF.awaitTermination()
  }

}
