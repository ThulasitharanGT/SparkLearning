package org.controller
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.kafka.common.header.Header
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.util.SparkOpener
import org.apache.spark.sql.functions._
object dStreamKafkaTry extends SparkOpener with Serializable {


  @transient def headersToHeaderCaseClass(headers:org.apache.kafka.common.header.Headers) =
    headers.toArray.toSeq.map(x => (x.key,x.value))

  val spark=SparkSessionLoc()
   spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  def main(args:Array[String]):Unit={
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("tmpApp")

    println(s"System.getProperty(checkpointForDStream) ${System.getProperty("checkpointForDStream")}")
    println(s"System.getProperty(StreamingSeconds) ${System.getProperty("StreamingSeconds")}")
    //     val streamingContext=new org.apache.spark.streaming.StreamingContext(sparkConf,Seconds(1))
    val streamingContext=new org.apache.spark.streaming.StreamingContext(spark.sparkContext,Seconds(System.getProperty("StreamingSeconds").toInt))
    //  val streamingContext=new org.apache.spark.streaming.StreamingContext(spark.sparkContext,Seconds(1))
    streamingContext.checkpoint("hdfs://localhost:8020/user/raptor/streams/checkpointDstream/")


    val kafkaParams1 = Map[String, Object](
      "bootstrap.servers" -> "localhost:8082,localhost:8083",
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaParams2 = Map[String, Object](
      "bootstrap.servers" -> "localhost:8082,localhost:8083",
      "key.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "value.deserializer" -> classOf[org.apache.kafka.common.serialization.StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream2",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic1 = Array("topicA")
    val stream1 = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topic1, kafkaParams1)
    ).map(x => (x.key, (x.value,x.offset,x.partition,
         headersToHeaderCaseClass(x.headers)
        )))

    // .map(x => (x.key,x.value))
    //  .map(x => (x.key,x.value,x.offset,x.partition,x.headers))

    val topic2 = Array("topicB")

    val stream2 = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topic2, kafkaParams2)
    ) // .map(x => (x.key,x.value))
      .map(x => (x.key, (x.value,x.offset,x.partition,
        headersToHeaderCaseClass(x.headers)
      )))
  //  .map(x => (x.key, (x.value,x.offset,x.partition,x.headers)))

  //  val joinedDStream=stream1.join(stream2)

    val joinedDStream=stream1.join(stream2)

    joinedDStream.foreachRDD( rdd => {
      val df= rdd.map(x => (x._1,x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._2._1,x._2._2._2,x._2._2._3,x._2._2._4))
        .toDF("key,valueTopicA,offsetTopicA,partitionTopicA,headersTopicA,valueTopicB,offsetTopicB,partitionTopicB,headersTopicB".split(",").toSeq:_*)

     val tmpDF= df.withColumn("explodedHeaderTopicA",explode(col("headersTopicA")))
        .withColumn("explodedHeaderTopicB",explode(col("headersTopicB")))
       .drop("headersTopicB","headersTopicA" )
       .selectExpr("*","explodedHeaderTopicA._1 as keyHeaderA"
         ,"cast(explodedHeaderTopicA._2 as string) valueHeaderA"
         ,"explodedHeaderTopicB._1 as keyHeaderB"
         ,"cast(explodedHeaderTopicB._2 as string) valueHeaderB")

      /* de duplicating will drop multiple header */
       //.withColumn("rankColumn" ,row_number.over(org.apache.spark.sql.expressions.Window.partitionBy("key"))).filter("rankColumn =1")

      tmpDF.printSchema
      tmpDF.show(false)
    })

    streamingContext.start
    streamingContext.awaitTermination
      // stream2.window(Seconds(10)).join(stream1.window(Seconds(20)))

    /*
    *
    * */

  }
/*

spark-shell --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2

import sys.process._
"hdfs dfs -rm -r hdfs://localhost:8020/user/raptor/streams/checkpointDstream/"!

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2 --class org.controller.dStreamKafkaTry --driver-memory 1g --driver-cores 2 --num-executors 1 --executor-cores 4 --executor-memory 1g --driver-java-options "-DStreamingSeconds=10 -DcheckpointForDStream=\"hdfs://localhost:8020/user/raptor/streams/checkpointDstream/\""    /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar

--driver-java-options "-DStreamingSeconds=10 -DcheckpointForDStream=\"hdfs://localhost:8020/user/raptor/streams/checkpointDstream/\""



val props=new java.util.Properties
props.put("bootstrap.servers","localhost:8082,localhost:8083")
props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

val kafkaProducer= new org.apache.kafka.clients.producer.KafkaProducer[String,String](props)

val newJavaIterator:java.lang.Iterable[org.apache.kafka.common.header.Header]= java.util.Arrays.asList(new org.apache.kafka.common.header.internals.RecordHeader("cool", "buddy".toList.map(_.toByte).toArray),new org.apache.kafka.common.header.internals.RecordHeader("cool1", "buddy2".toList.map(_.toByte).toArray))

val producerRecord=new org.apache.kafka.clients.producer.ProducerRecord[String,String]("topicA",0,"key1","value1")
// cast it to java.lang.Iterable and then pass it
// java.util.Arrays.asList(new org.apache.kafka.common.header.internals.RecordHeader("cool", "buddy".toList.map(_.toByte).toArray))


val producerRecordA=new org.apache.kafka.clients.producer.ProducerRecord[String,String]("topicA","key1","value1")
newJavaIterator.forEach(x => producerRecordA.headers.add(x))
kafkaProducer.send(producerRecordA)

val producerRecordB=new org.apache.kafka.clients.producer.ProducerRecord[String,String]("topicB",0,"key1","value1",newJavaIterator)
kafkaProducer.send(producerRecordB)*/

}
