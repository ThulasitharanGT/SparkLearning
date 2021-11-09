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


}
