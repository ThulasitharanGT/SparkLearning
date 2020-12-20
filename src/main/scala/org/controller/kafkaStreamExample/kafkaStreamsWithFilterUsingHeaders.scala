package org.controller.kafkaStreamExample

import org.util.SparkOpener
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object kafkaStreamsWithFilterUsingHeaders extends SparkOpener{

  def filterStringProcessor(itemsString:String)={
    var finalOutput=""
    var itemNum=0
    for (i <- 1 to itemsString.split(",").size)
      itemNum match {
        case value if value ==0 => finalOutput=finalOutput+ s"'${itemsString.split(",")(i-1)}'"
          itemNum=itemNum+1
        case _ => finalOutput=finalOutput+ s",'${itemsString.split(",")(i-1)}'"
          itemNum=itemNum+1
      }
    finalOutput
  }

  val spark=SparkSessionLoc("temp")

  def main(args:Array[String])={

    val selectExprCols="cast(key as string) as key,cast(value as string),topic,partition,offset,timestamp,timestampType,explode(headers) as headers".split(",")

    val streamOneFilterSet="fp1,fp2,fp3"
    val streamTwoFilterSet="q1,q2,q3"

    // if messages are sent without header, they will not be processed.

    val readStreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9094,localhost:9093,localhost:9092")/*.option("failOnDataLoss","false")*/.option("includeHeaders", "true").option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("subscribe","TopicTest").option("offset","earliest").load.selectExpr(selectExprCols:_*).withColumn("headerKey",col("headers.key")).withColumn("headerValue",col("headers.value").cast(StringType)).drop("headers").filter(s"headerKey in (${filterStringProcessor(streamOneFilterSet)})")

    readStreamDF.writeStream.format("console").outputMode("append").option("checkpointLocation","hdfs://localhost:8020/user/raptor/stream/tmpCheck6/").start


    // if messages are sent without header, they will not be processed.

    val readStreamDF2=spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9094,localhost:9093,localhost:9092")/*.option("failOnDataLoss","false")*/.option("includeHeaders", "true").option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer").option("subscribe","TopicTest").option("offset","earliest").load.selectExpr(selectExprCols:_*).withColumn("headerKey",col("headers.key")).withColumn("headerValue",col("headers.value").cast(StringType)).drop("headers").where(s"headerKey in (${filterStringProcessor(streamTwoFilterSet)})")

    readStreamDF2.writeStream.format("console").outputMode("append").option("checkpointLocation","hdfs://localhost:8020/user/raptor/stream/tmpCheck7/").start

  spark.streams.awaitAnyTermination
  }

}
