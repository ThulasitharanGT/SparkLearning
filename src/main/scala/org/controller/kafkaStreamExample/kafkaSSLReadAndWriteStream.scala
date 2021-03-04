package org.controller.kafkaStreamExample


import org.constants.projectConstants
import org.util.SparkOpener
import org.util.readWriteUtil.{loadProperties, writeStreamAsDelta}
import sys.process._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object kafkaSSLReadAndWriteStream extends SparkOpener{
def main(args:Array[String]):Unit={
/*
  val filesTemp=Files.getLastModifiedTime(Paths.get("/home/raptor/Softwares/kafka_2.12-2.7.0/keys/client.keystore.jks"))
  val filesTempMillis=Files.getLastModifiedTime(Paths.get("/home/raptor/Softwares/kafka_2.12-2.7.0/keys/client.keystore.jks")).toMillis
*/

  val spark=SparkSessionLoc()
val inputMap=collection.mutable.Map[String,String]()
  println(s"CLA's received")
  for (arg <- args)
    {
      val keyPart=arg.split("=",2)(0)
      val valPart=arg.split("=",2)(1)
      println(s"keyPart = ${keyPart} valPart = ${valPart}")
      inputMap.put(keyPart,valPart)
    }
    val checkpointLocation=inputMap("checkpointLocation")
    val clearCheckpointFlag=inputMap("clearCheckpointFlag")
    val propFileLocation=inputMap("propFileLocation") // hdfs:///user/raptor/hadoop/keys/keysForCLI.txt
    val topic=inputMap("topic")
    val offset=inputMap("offset")
    val broker=inputMap("broker")
    val kafkaTrustStoreType=inputMap("kafkaTrustStoreType")
    val keyDeSerializer=inputMap("keyDeSerializer")
    val valueDeSerializer=inputMap("valueDeSerializer")
    val persistPath=inputMap("persistPath")
    val prop = loadProperties(propFileLocation)
    val kafkaSecurityProtocol= inputMap("kafkaSecurityProtocol")

    println(s"Checking if checkpoint exists in ${checkpointLocation}")
    val checkpointExists=s"hdfs dfs -ls ${checkpointLocation}"! ; // run time error might occur if you fail to terminate the statement
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
        case value if value !=0 =>
          println(s"Checkpoint does not exists in ${checkpointLocation}")
      }

    /*

 spark-submit --class org.controller.kafkaStreamExample.kafkaSSLReadAndWriteStream --driver-cores 2 --driver-memory 2g --executor-memory 1g --executor-cores 2 --num-executors 2 --conf spark.dynamic.memory.allocation.enabled=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.7.0  /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkpointLocation=hdfs://localhost:8020/user/raptor/streams/tmp/ propFileLocation=hdfs://localhost:8020/user/raptor/hadoop/keys/keysForCLI.txt topic=tmpTopic offset=earliest broker="raptor-VirtualBox:9194,raptor-VirtualBox:9195" kafkaTrustStoreType=jks keyDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" valueDeSerializer="org.apache.kafka.common.serialization.StringDeserializer" persistPath="hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmp1/" clearCheckpointFlag=y kafkaSecurityProtocol=SSL

./bin/kafka-console-producer.sh --topic tmpTopic --broker-list raptor-VirtualBox:9194,raptor-VirtualBox:9195 --producer.config ./keys/keysForCLI.txt  --property parse.key=true --property key.separator=:

val deltaTable=DeltaTable.forPath(spark,"hdfs://localhost:8020/user/raptor/hadoop/deltaLake/tmp1/")

deltaTable.toDF.show(false)

*/
  println(" prop.getProperty(\"ssl.truststore.location\") "+prop.getProperty("ssl.truststore.location"))
  println(" prop.getProperty(\"ssl.truststore.password\") "+prop.getProperty("ssl.truststore.password"))
  println(" prop.getProperty(\"ssl.keystore.location\") "+prop.getProperty("ssl.keystore.location"))
  println(" prop.getProperty(\"ssl.keystore.password\") "+prop.getProperty("ssl.keystore.password"))
  println(" prop.getProperty(\"ssl.key.password\") "+prop.getProperty("ssl.key.password"))

 // val columnSeq="cast (key as string) key, cast (value as string) value, cast (topic as string) topic, cast (partition as string) partition, cast (offset as string) offset, cast (timestamp as string) timestamp, cast (timestampType as string) timestampType".split(",").toSeq
  val readStreamDF=spark.readStream.format("kafka").option("key.deserializer",keyDeSerializer).option("value.deserializer",valueDeSerializer).option("subscribe",topic).option("offsets",offset).option("kafka.bootstrap.servers",broker).option("kafka.ssl.endpoint.identification.algorithm", "").option("kafka.ssl.truststore.location", prop.getProperty("ssl.truststore.location")).option("kafka.ssl.truststore.password", prop.getProperty("ssl.truststore.password")/*"test123"*/).option("kafka.security.protocol", kafkaSecurityProtocol).option("kafka.ssl.truststore.type", kafkaTrustStoreType/*"jks"*/).option("kafka.ssl.keystore.location", prop.getProperty("ssl.keystore.location")).option("kafka.ssl.keystore.password", prop.getProperty("ssl.keystore.password")/*"test123"*/).option("kafka.ssl.key.password", prop.getProperty("ssl.key.password")/*"test123"*/).load//.selectExpr(columnSeq:_*)
    inputMap.put(projectConstants.outputModeArg,projectConstants.fileAppendValue)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.deltaFormat)
    inputMap.put(projectConstants.checkPointLocationArg,checkpointLocation)
    inputMap.put(projectConstants.deltaMergeSchemaClause,projectConstants.stringTrue) // stringFalse
    inputMap.put(projectConstants.deltaOverWriteSchemaClause,projectConstants.stringTrue) //stringFalse
    inputMap.put(projectConstants.pathArg,persistPath)
    writeStreamAsDelta(spark,inputMap,readStreamDF.select(col("key").cast(StringType),col("value").cast(StringType),col("topic").cast(StringType),col("partition").cast(StringType),col("offset").cast(StringType),col("timestamp").cast(StringType),col("timestampType").cast(StringType))).start

    spark.streams.awaitAnyTermination
    /*
 ssl.keystore.location=/home/raptor/Softwares/kafka_2.12-2.7.0/keys/kafka.keystore.jks
ssl.keystore.password=test123
ssl.key.password=test123
ssl.truststore.location=/home/raptor/Softwares/kafka_2.12-2.7.0/keys/kafka.truststore.jks
ssl.truststore.password=test123
security.protocol=SSL
*/
  }



}
