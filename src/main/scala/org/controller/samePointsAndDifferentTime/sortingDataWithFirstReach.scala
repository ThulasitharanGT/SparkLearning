package org.controller.samePointsAndDifferentTime

import org.constants.projectConstants
import org.util.SparkOpener
import org.util.readWriteUtil.{checkPointLocationCleaner, loadProperties, readStreamFunction, writeStreamAsDelta}

import sys.process._
/*
spark-submit --class org.controller.samePointsAndDifferentTime.sortingDataWithFirstReach --driver-cores 2 --driver-memory 2g --executor-memory 1g --executor-cores 2 --num-executors 2 --conf spark.dynamic.memory.allocation.enabled=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0  /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tmp/ propFileLocation=hdfs://localhost:8020/user/raptor/hadoop/keys/keysForCLI.txt topic=tmpTopic offset=earliest broker="raptor-VirtualBox:9194,raptor-VirtualBox:9195" kafkaTrustStoreType=jks keyDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" valueDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" persistPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmp2/" clearCheckpointFlag=y kafkaSecurityProtocol=SSL
spark-submit --class org.controller.samePointsAndDifferentTime.deltaStreamToUpdateTheFinalTable --driver-cores 2 --driver-memory 2g --executor-memory 1g --executor-cores 2 --num-executors 2 --conf spark.dynamic.memory.allocation.enabled=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0  /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tmp2/ propFileLocation=hdfs://localhost:8020/user/raptor/hadoop/keys/keysForCLI.txt topic=tmpTopic offset=earliest broker="raptor-VirtualBox:9194,raptor-VirtualBox:9195" kafkaTrustStoreType=jks keyDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" valueDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" inputPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmp2/"  persistPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmpFinal/"  clearCheckpointFlag=y kafkaSecurityProtocol=SSL persistAggPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmpFinalAgg/"

./bin/kafka-console-producer.sh --topic tmpTopic --broker-list raptor-VirtualBox:9194,raptor-VirtualBox:9195 --producer.config ./keys/keysForCLI.txt  --property parse.key=true --property key.separator=:

// tmp KafkaInputs
ty:Max,20,2020-01-04 11:22:33.667
ty:Max,10,2020-01-05 11:22:33.627
vy:Vettel,20,2020-01-04 11:22:33.657
ty:Vettel,10,2020-01-04 11:22:33.696
ty:Senna,50,2020-01-08 11:22:33.696
ty:Senna,10,2020-01-08 11:22:33.899
ty:Senna,10,2020-01-09 11:22:33.899
// read from that delta stream and insert into anther table. use that table as base and over write data

1)reads from kafka and persists in bronze lake.
2)reads from bronze lake and updates the bronze level data
--> updates two tables
2.1>little bit of aggregation of points and max timestamp
2.2>then the first point scored person will be shown first in AGG part , this is done by sorting accrding to asc of timestamp and desc of points. If tie , first person scored that point gets 1st priority


*/
object sortingDataWithFirstReach extends SparkOpener {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionLoc()
    val inputMap = collection.mutable.Map[String, String]()
    for (arg <- args) {
      val keyPart = arg.split("=", 2)(0)
      val valPart = arg.split("=", 2)(1)
      inputMap.put(keyPart, valPart)
    }
    val checkpointLocation = inputMap("checkpointLocation")
    val clearCheckpointFlag = inputMap("clearCheckpointFlag")
    val propFileLocation = inputMap("propFileLocation") // hdfs:///user/raptor/hadoop/keys/keysForCLI.txt
    val topic = inputMap("topic")
    val offset = inputMap("offset")
    val broker = inputMap("broker")
    val kafkaTrustStoreType = inputMap("kafkaTrustStoreType")
    val keyDeSerializer = inputMap("keyDeSerializer")
    val valueDeSerializer = inputMap("valueDeSerializer")
    val persistPath = inputMap("persistPath")
    val prop = loadProperties(propFileLocation)
    val kafkaSecurityProtocol = inputMap("kafkaSecurityProtocol")

    println(s"Checking if checkpoint exists in ${checkpointLocation}")
   /* val checkpointExists = s"hdfs dfs -ls ${checkpointLocation}" !; // run time error might occur if you fail to terminate the statement
    checkpointExists match {
      case value if value == 0 =>
        println(s"Checkpoint exists in ${checkpointLocation}")
        clearCheckpointFlag match {
          case value if value.toUpperCase == "Y" =>
            val checkpointClear = s"hdfs dfs -rm -r ${checkpointLocation}" !;
            checkpointClear match {
              case value if value == 0 =>
                println(s"Checkpoint cleared  in ${checkpointLocation}")
              case value if value != 0 =>
                println(s"Error in clearing checkpoint  ${checkpointLocation}")
            }
          case value if value.toUpperCase == "N" =>
            println(s"Checkpoint wont be cleared  in ${checkpointLocation}")
          case value =>
            println(s"Invalid option for clearCheckpointFlag - ${value}")
        }
      case value if value != 0 =>
        println(s"Checkpoint does not exists in ${checkpointLocation}")
    }*/

    checkPointLocationCleaner(checkpointLocation,clearCheckpointFlag)
    /*  println(" prop.getProperty(\"ssl.truststore.location\") "+prop.getProperty("ssl.truststore.location"))
      println(" prop.getProperty(\"ssl.truststore.password\") "+prop.getProperty("ssl.truststore.password"))
      println(" prop.getProperty(\"ssl.keystore.location\") "+prop.getProperty("ssl.keystore.location"))
      println(" prop.getProperty(\"ssl.keystore.password\") "+prop.getProperty("ssl.keystore.password"))
      println(" prop.getProperty(\"ssl.key.password\") "+prop.getProperty("ssl.key.password"))*/
    inputMap.put(projectConstants.fileFormatArg, projectConstants.kafkaSSLFormat)
    inputMap.put(projectConstants.kafkaKeyDeserializerArg, keyDeSerializer)
    inputMap.put(projectConstants.kafkaValueDeserializerArg, valueDeSerializer)
    inputMap.put(projectConstants.subscribeArg, topic)
    inputMap.put(projectConstants.startingOffsetsArg, offset)
    inputMap.put(projectConstants.kafkaBootStrapServersArg, broker)
    inputMap.put(projectConstants.kafkaSecurityProtocolArg, kafkaSecurityProtocol)
    inputMap.put(projectConstants.kafkaSSLEndpointIdentificationAlgorithmArg, "")
    inputMap.put(projectConstants.kafkaSSLTrustStoreLocationArg, prop.getProperty("ssl.truststore.location"))
    inputMap.put(projectConstants.kafkaSSLTrustStorePasswordArg, prop.getProperty("ssl.truststore.password"))
    inputMap.put(projectConstants.kafkaSSLKeyStoreLocationArg, prop.getProperty("ssl.keystore.location"))
    inputMap.put(projectConstants.kafkaSSLKeyStorePasswordArg, prop.getProperty("ssl.keystore.password"))
    inputMap.put(projectConstants.kafkaSSLKeyPasswordArg, prop.getProperty("ssl.key.password"))
    inputMap.put(projectConstants.kafkaSSLTruststoreTypeArg, kafkaTrustStoreType)

    val readStreamDF = readStreamFunction(spark, inputMap)
    inputMap.put(projectConstants.outputModeArg, projectConstants.fileAppendValue)
    inputMap.put(projectConstants.fileFormatArg, projectConstants.deltaFormat)
    inputMap.put(projectConstants.checkPointLocationArg, checkpointLocation)
    inputMap.put(projectConstants.deltaMergeSchemaClause, projectConstants.stringTrue) // stringFalse
    inputMap.put(projectConstants.deltaOverWriteSchemaClause, projectConstants.stringTrue) //stringFalse
    inputMap.put(projectConstants.pathArg, persistPath)

    val columnSeq = "cast (key as string) key,cast (split(cast (value as string),',')[0] as string) driverName,cast (split(cast (value as string),',')[1] as string) points,cast (split(cast (value as string),',')[2] as timestamp) receivedTimeStamp ,cast (topic as string) topic,cast (partition as string) partition,cast (offset as string) offset,cast (timestamp as string) timestamp,cast (timestampType as string) timestampType".split(",cast").map(x => x.trim.startsWith("(") match {
      case value if value == true => s"cast${x}"
      case value if value == false => x
    }).toSeq
    writeStreamAsDelta(spark, inputMap, readStreamDF.selectExpr(columnSeq: _*)).start

    spark.streams.awaitAnyTermination

  }
}
