package org.controller.kafkaStreamExample

import org.util.SparkOpener
import org.constants.projectConstants

/*
cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

spark-submit --class org.controller.kafkaStreamExample.deltaStreamRead  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages io.delta:delta-core_2.11:0.5.0  SparkLearning-1.0-SNAPSHOT.jar inputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/ checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpointCarSensorDataFromStream/ outputPath=hdfs://localhost/user/raptor/kafka/stream/temp/consoleCarSensorDataFromStream/
 */
object deltaStreamRead extends SparkOpener{
  val spark=SparkSessionLoc("delta Stream TableRead")
  spark.sparkContext.setLogLevel("ERROR")
def main(args:Array[String]):Unit={
  val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
  for (arg<-args)
  {
    val keyPart=arg.split("=",2)(0)
    val valPart=arg.split("=",2)(1)
    inputMap.put(keyPart,valPart)
  }
  val inputPath=inputMap("inputPath") //"file:///home/raptor/output/kafkaTableDump/carSensorBronze" hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/
  val checkPointLocation=inputMap("checkPointLocation")//"file:///home/raptor/checkpointStream/checkpoint1" checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpointCarSensorDataFromStream/
  val outputPath=inputMap("outputPath") // outputPath=hdfs://localhost/user/raptor/kafka/stream/temp/consoleCarSensorDataFromStream/
  val deltaSteamDF= spark.readStream.format(projectConstants.deltaFormat).load(inputPath)
  println("--------------------->stream read<------------------------")
  val queryStreamStartDF=deltaSteamDF.writeStream.outputMode("append").format("console").option("checkpointLocation",checkPointLocation).option("path",outputPath)
  println("--------------------->stream write object created<------------------------")
  queryStreamStartDF.start().awaitTermination()
}
}
