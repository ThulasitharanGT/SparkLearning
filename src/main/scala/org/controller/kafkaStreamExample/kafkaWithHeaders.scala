package org.controller.kafkaStreamExample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.util.SparkOpener

class kafkaWithHeaders extends SparkOpener{

val spark=SparkSessionLoc("temp")
  def main(args:Array[String])={

    val selectExprCols="cast(key as string) as key,cast(value as string),topic,partition,offset,timestamp,timestampType,explode(headers) as headers".split(",")

    // if messages are sent without header, they will not be processed.

    val readStreamWithHeaderDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9094,localhost:9093,localhost:9092")/*.option("failOnDataLoss","false")*/.option("includeHeaders", "true").option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("subscribe","TopicTest").option("offset","earliest").load.selectExpr(selectExprCols:_*).withColumn("headerKey",col("headers.key")).withColumn("headerValue",col("headers.value").cast(StringType)).drop("headers")

    readStreamWithHeaderDF.writeStream.format("console").outputMode("append").option("checkpointLocation","hdfs://localhost:8020/user/raptor/stream/tmpCheck6/").start

    // without header

    val selectExprColsWithoutHeader="cast(key as string) as key,cast(value as string),topic,partition,offset,timestamp,timestampType".split(",")

    val readStreamWithoutHeaderDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9094,localhost:9093,localhost:9092")/*.option("failOnDataLoss","false")*/.option("includeHeaders", "true").option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("subscribe","TopicTest").option("offset","earliest").load.selectExpr(selectExprColsWithoutHeader:_*)

    readStreamWithoutHeaderDF.writeStream.format("console").outputMode("append").option("checkpointLocation","hdfs://localhost:8020/user/raptor/stream/tmpCheck6/").start

    spark.streams.awaitAnyTermination()

  }


}
