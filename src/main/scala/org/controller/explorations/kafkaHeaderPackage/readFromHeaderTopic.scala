package org.controller.explorations.kafkaHeaderPackage

import org.util.SparkOpener
import org.apache.spark.sql.functions._

object readFromHeaderTopic extends SparkOpener{
  val spark=SparkSessionLoc()
  val inputMap=collection.mutable.Map[String,String]()
  def initializeInputMap(args:Array[String])= for (arg <- args)
    inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

  def initializeInputMapUpdated(args:Array[String])= args.map(x=> inputMap.put(x.split("=",2)(0),x.split("=",2)(1)))
  def initializeInputMapProperlyUpdated(args:Array[String])= args.map(_.split("=",2)).map(x=> inputMap.put(x(0),x(1)))

  def main(args:Array[String]):Unit ={
    initializeInputMapProperlyUpdated(args)
  /*


spark-submit --class org.controller.explorations.kafkaHeaderPackage.readFromHeaderTopic --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --num-executors 2 --executor-memory 1g --executor-cores 2 --driver-cores 4 --driver-memory 1g --conf spark.sql.shuffle.partitions=4 --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1000m /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServer="localhost:8082,localhost:8083,localhost:8084" topic=tmpTopic startingOffsets=latest writeStreamFormat=console outputMode=update checkpointLocation=hdfs://localhost:8020/user/raptor/kafka/tmpStream3



 */
    spark.readStream.format("kafka").option("kafka.bootstrap.servers",inputMap("bootstrapServer")).option("subscribe",inputMap("topic"))
      .option("includeHeaders","true").option("startingOffsets",inputMap("startingOffsets"))
      .load.selectExpr("cast (key as string) key","cast (value as string) value","topic","partition","offset","timestamp","timestampType","headers").withColumn("headersExploded",explode(col("headers"))).selectExpr("*","headersExploded.key as headerKey","cast(headersExploded.value as string) headerValue").withColumn("headersMapConverted",map(col("headerKey"),col("headerValue"))).groupBy("key","value","topic","partition","offset","timestamp","timestampType","headers").agg(collect_list("headersMapConverted").as("headersConverted")).writeStream.format(inputMap("writeStreamFormat")).outputMode(inputMap("outputMode")).option("truncate","false").option("checkpointLocation",inputMap("checkpointLocation")).start

    spark.streams.awaitAnyTermination
}
}
