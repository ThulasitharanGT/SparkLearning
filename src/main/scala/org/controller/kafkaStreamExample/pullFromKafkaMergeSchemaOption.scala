package org.controller.kafkaStreamExample

import org.constants.projectConstants
import org.util.{SparkOpener, readWriteUtil}

object pullFromKafkaMergeSchemaOption extends SparkOpener {
  val spark=SparkSessionLoc("readStream for topic")
  def main(args:Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg<-args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))//key,value
    val bootStrapServers=inputMap("bootStrapServers")
    val inputCheckPointLocation=inputMap("inputCheckPointLocation")
    val outputCheckPointLocation=inputMap("outputCheckPointLocation")
    val outputPath=inputMap("outputPath")
    val kafkaKeyDeserializer=inputMap("kafkaKeyDeserializer")
    val kafkaValueDeserializer=inputMap("kafkaValueDeserializer")
    val startingOffset=inputMap("startingOffset")
    val topic=inputMap("topic")
    val outputMode=inputMap("outputMode")
    val mergeSchema=inputMap("mergeSchema")
    val overWriteSchema=inputMap("overWriteSchema")
    val outputWriteFormat=inputMap("outputWriteFormat")

    inputMap.put(projectConstants.fileFormatArg,projectConstants.kafkaFormat)
    inputMap.put(projectConstants.kafkaBootStrapServersArg,bootStrapServers)
    inputMap.put(projectConstants.checkPointLocationArg,inputCheckPointLocation)
    inputMap.put(projectConstants.kafkaKeyDeserializerArg,kafkaKeyDeserializer)
    inputMap.put(projectConstants.kafkaValueDeserializerArg,kafkaValueDeserializer)
    inputMap.put(projectConstants.startingOffsetsArg,startingOffset)
    inputMap.put(projectConstants.subscribeArg,topic)
   val readStreamDF=readWriteUtil.readStreamFunction(spark,inputMap)
    inputMap.put(projectConstants.outputModeArg,outputMode)
    inputMap.put(projectConstants.fileFormatArg,outputWriteFormat)
    inputMap.put(projectConstants.checkPointLocationArg,outputCheckPointLocation)
    inputMap.put(projectConstants.pathArg,outputPath)
    inputMap.put(projectConstants.deltaMergeSchemaClause,mergeSchema)
    inputMap.put(projectConstants.deltaOverWriteSchemaClause,overWriteSchema)

    mergeSchema.toLowerCase match {
   case projectConstants.stringFalse =>{ //4 cols
     val writeStream=readWriteUtil.writeStreamFunction(spark,inputMap,readStreamDF.selectExpr("*","split(value,'~') as valueSplitted").drop("value").selectExpr("offset", "topic", "timestamp", "cast(valueSplitted[0] as string)as value", "cast(valueSplitted[1] as string) as date", "cast(valueSplitted[2] as string) as raceTrack", "cast(valueSplitted[3] as string) as runType", "timestampType", "partition", "cast (key  as string) as key")).start
     writeStream.awaitTermination()
   }
   case projectConstants.stringTrue =>{ //5 cols in msg
     val writeStream=readWriteUtil.writeStreamAsDelta(spark,inputMap,readStreamDF.selectExpr("*","split(value,'~') as valueSplitted").drop("value").selectExpr("offset", "topic", "timestamp", "cast(valueSplitted[0] as string)as value", "cast(valueSplitted[1] as string) as date", "cast(valueSplitted[2] as string) as tyre", "cast(valueSplitted[3] as string) as raceTrack", "cast(valueSplitted[4] as string) as runType", "timestampType", "partition", "cast (key  as string) as key")).start
     writeStream.awaitTermination()
   }
   case _ => println("not a valid mergeSchema Option")
 }

  }

}
