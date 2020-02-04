package org.controller.kafkaStreamExample

import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants
/*
for 240 spark --- works for streaming queries ty as a project

spark-shell --packages org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.11:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,io.delta:delta-core_2.11:0.5.0

// writing -- kafka producer pushes data to a topic. below query hits that
val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("startingOffsets", "earliest").option("subscribe", "CarSensor").load()
val query = df.writeStream.outputMode("append").format("delta").option("checkpointLocation","file:///home/raptor/checkpointStream/checkpoint").option("path","file:///home/raptor/output/kafkaTableDump/carSensorBronze").partitionBy("key").start()

// reading
val queryStream= spark.readStream.format("delta").load("file:///home/raptor/output/kafkaTableDump/carSensorBronze").selectExpr("*","split(value,'~') as valueSplitted").drop("value").selectExpr("offset", "topic", "timestamp", "cast(valueSplitted[0] as string)as value", "cast(valueSplitted[1] as string) as date", "timestampType", "partition", "key")
val queryStreamDF=queryStream.writeStream.outputMode("append").format("console").option("checkpointLocation","file:///home/raptor/checkpointStream/checkpoint1").option("path","file:///home/raptor/output/kafkaTableDump/carSensorBronze1").start()


cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

spark-submit --class org.controller.kafkaStreamExample.kafkaStreamRead  --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-cores 1 --driver-memory 1g --packages org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.11:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,io.delta:delta-core_2.11:0.5.0  SparkLearning-1.0-SNAPSHOT.jar bootStrapServer=localhost:9092,localhost:9093,localhost:9094 keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer topicName=CarSensor checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/checkPointForStreamPullAndDeltaPushJob outputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/


bootStrapServer=localhost:9092,localhost:9093,localhost:9094 keyDeserializer=org.apache.kafka.common.serialization.StringDeserializer valueDeserializer=org.apache.kafka.common.serialization.StringDeserializer topicName=CarSensor checkPointLocation=hdfs://localhost/user/raptor/kafka/stream/checkpoint/ outputPath=hdfs://localhost/user/raptor/kafka/stream/output/carSensorDataFromStream/
*/
object kafkaStreamRead extends SparkOpener{
  val spark=SparkSessionLoc("Kafka stream using spark ")
  def main(args: Array[String]): Unit = {
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
    val checkPointLocation=inputMap("checkPointLocation")
    val outputPath=inputMap("outputPath")

    inputMap.put(projectConstants.kafkaBootStrapServersArg,bootStrapServer)
    inputMap.put(projectConstants.kafkaValueDeserializerArg,valueDeserializer)
    inputMap.put(projectConstants.kafkaKeyDeserializerArg,keyDeserializer)
    inputMap.put(projectConstants.subscribeArg,topicName)// topic name
    inputMap.put(projectConstants.startingOffsetsArg,"earliest")
    inputMap.put(projectConstants.fileFormatArg,projectConstants.kafkaFormat)
    val readStreamDF = readWriteUtil.readStreamFunction(spark,inputMap).selectExpr("*","split(value,'~') as valueSplitted").drop("value").selectExpr("offset", "topic", "timestamp", "cast(valueSplitted[0] as string)as value", "cast(valueSplitted[1] as string) as date", "timestampType", "partition", "cast (key  as string) as key")
    // spark.readStream.format(projectConstants.kafkaFormat).option(projectConstants.kafkaBootStrapServersArg, bootStrapServer).option(projectConstants.kafkaValueDeserializerArg,valueDeserializer).option(projectConstants.kafkaKeyDeserializerArg,keyDeserializer).option("startingOffsets", "earliest").option("subscribe", topicName).load().selectExpr("*","split(value,'~') as valueSplitted").drop("value").selectExpr("offset", "topic", "timestamp", "cast(valueSplitted[0] as string)as value", "cast(valueSplitted[1] as string) as date", "timestampType", "partition", "cast (key  as string) as key")
    inputMap.put(projectConstants.fileFormatArg,projectConstants.deltaFormat)
    inputMap.put(projectConstants.checkPointLocationArg,checkPointLocation)
    inputMap.put(projectConstants.pathArg,outputPath)
    inputMap.put(projectConstants.outputModeArg,"append")

    val writeStreamDF = readWriteUtil.writeStreamFunction(spark,inputMap,readStreamDF).partitionBy("date","topic","partition","key").start()
      //readStreamDF.writeStream.outputMode("append").format(projectConstants.deltaFormat).option("checkpointLocation",checkPointLocation).option("path",outputPath).partitionBy("date","topic","partition","key").start()
    writeStreamDF.awaitTermination()
  }
}
