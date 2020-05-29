package org.controller.twitterPullTry

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.{Calendar, Date, Properties}
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants._
object twitterDataPullFromKafka extends SparkOpener{

  val KafkaTopic= "bigdata-tweets"
  val KafkaServers="localhost:9092,localhost:9093"
  val tweetDelimiter="~~Tweet-Delimiter~~"
  val userDelimiter="~~User-Delimiter~~"
  val schema = new StructType().add("id",LongType).add("text",StringType).add("lang",StringType).add("user",StringType).add("retweetCount",IntegerType)add("favoriteCount",IntegerType)
  val dateFormat = new SimpleDateFormat("YYYYMMddHHmmSS")
  val spark=SparkSessionLoc("temp")

  def main(args:Array[String]):Unit={

    val inputMap:collection.mutable.Map[String,String]= collection.mutable.Map[String,String]()
    for (arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val valPart=arg.split("=",2)(1)
        inputMap.put(keyPart,valPart)
      }
    val initialColumnsFromStreamSplittingTweets=Seq("CAST (key AS STRING) key",s"split(CAST (value AS STRING),'${tweetDelimiter}') value","CAST (topic AS STRING) topic","CAST (partition AS INT) partition","CAST (offset AS INT) offset","CAST (timestamp  AS timestamp) timestamp","CAST (timestampType AS INT) timestampType")
    val tweetColumnsSelectExprSelectingTweetsAndSplittingUser=Seq("CAST (key as long) key","topic","partition","offset","timestamp","timestampType","value[0] as tweetId","value[1] as tweetText","value[2] as tweetLang",s" split(value[3],'${userDelimiter}') as userInfoArray","value[4] as tweetReTweetCount","value[5] as tweetFavoriteCount")
    val tweetFinalColumnsSelectExprSelectingTweetsAndUser=Seq("key","topic","partition","offset","timestamp","timestampType","tweetId","tweetText","tweetLang","userInfoArray[0] as userId","userInfoArray[1] as userName","userInfoArray[2] as userScreenName","userInfoArray[3] as userLocation","userInfoArray[4] as userFollowersCount","tweetReTweetCount","tweetFavoriteCount")

    inputMap.put(fileFormatArg,kafkaFormat)
    inputMap.put(checkPointLocationArg,inputMap("checkPointLocationRead"))
    inputMap.put(kafkaBootStrapServersArg,inputMap(kafkaBootStrapServersArg))
    inputMap.put(kafkaValueDeserializerArg,inputMap(kafkaValueDeserializerArg))
    inputMap.put(kafkaKeyDeserializerArg,inputMap(kafkaKeyDeserializerArg))
    inputMap.put(startingOffsetsArg,inputMap(startingOffsetsArg))
    inputMap.put(subscribeArg,inputMap("topic"))
    val readStreamDF=readWriteUtil.readStreamFunction(spark,inputMap).selectExpr(initialColumnsFromStreamSplittingTweets:_*).selectExpr(tweetColumnsSelectExprSelectingTweetsAndSplittingUser:_*).selectExpr(tweetFinalColumnsSelectExprSelectingTweetsAndUser:_*)
    inputMap.put(outputModeArg,inputMap("outputMode"))
    inputMap.put(fileFormatArg,inputMap("outputFileFormat"))
    inputMap.put(checkPointLocationArg,inputMap("checkPointLocationWrite"))
    inputMap.put(pathArg,inputMap("outputLocation"))
    val writeStreamDf=readWriteUtil.writeStreamFunction(spark,inputMap,readStreamDF)
    writeStreamDf.start.awaitTermination

    // spark-submit --class org.controller.twitterPullTry.twitterDataPullFromKafka --driver-memory 1g --executor-memory 1g --driver-cores 2 --executor-cores 2 --num-executors 2 --deploy-mode client /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar topic=bigdata-tweets startingOffsets=earliest outputMode=append outputFileFormat=parquet checkPointLocationWrite=/user/raptor/kafka/checkpoint/twitterWriteCheckpoint outputLocation=/user/raptor/kafka/output/twitterTweets/ checkPointLocationRead=/user/raptor/kafka/checkpoint/twitterReadCheckpoint  kafka.bootstrap.servers= value.deserializer=org.apache.kafka.common.serialization.StringDeserializer key.deserializer=org.apache.kafka.common.serialization.StringDeserializer

/*  Tried out of the box
    // reading entire data from stream with all kafka metadata
    spark.readStream.format("kafka").option("kafka.bootstrap.servers",KafkaServers).option("subscribe", KafkaTopic).option("startingOffsets","earliest").load.selectExpr("CAST (key AS long) key","CAST (value AS Tweet) value","CAST (topic AS STRING) topic","CAST (partition AS INT) partition","CAST (offset AS INT) offset","CAST (timestamp  AS timestamp) timestamp","CAST (timestampType AS INT) timestampType").writeStream.format("console").option("checkpointLocation","/use/temp/someLocation2/").start
    val dateValue1=dateFormat.format(Calendar.getInstance().getTime())

    // reading value alone
    spark.readStream.format("kafka").option("kafka.bootstrap.servers",KafkaServers).option("subscribe", KafkaTopic).option("startingOffsets","earliest").load.selectExpr("CAST (value AS STRING)").writeStream.format("console").option("checkpointLocation",s"/use/temp/someLocation${dateValue1}/").start


    // from jason wont work in special charecter's . returns null. Encoding problem.
    val dateValue2=dateFormat.format(Calendar.getInstance().getTime())
    val readstreamDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers",KafkaServers).option("subscribe", KafkaTopic).option("startingOffsets","earliest").load
    readstreamDF.selectExpr("CAST (value AS STRING)").select(from_json(col("value"),schema).as("data")).selectExpr("data.*").writeStream.format("console").option("charset", "UTF-8").option("parserLib", "univocity").option("multiLine", "true").option("checkpointLocation",s"/use/temp/someLocation${dateValue2}/").start


    // adding custom delimiter and working around
    //select expression columns
    val initialColumnsFromStream=Seq("CAST (key AS STRING) key",s"split(CAST (value AS STRING),'${tweetDelimiter}') value","CAST (topic AS STRING) topic","CAST (partition AS INT) partition","CAST (offset AS INT) offset","CAST (timestamp  AS timestamp) timestamp","CAST (timestampType AS INT) timestampType")
    val tweetColumnsSelectExpr=Seq("CAST (key as long) key","topic","partition","offset","timestamp","timestampType","value[0] as tweetId","value[1] as tweetText","value[2] as tweetLang",s" split(value[3],'${userDelimiter}') as userInfoArray","value[4] as tweetReTweetCount","value[5] as tweetFavoriteCount")
    val tweetFinalColumnsSelectExpr=Seq("key","topic","partition","offset","timestamp","timestampType","tweetId","tweetText","tweetLang","userInfoArray[0] as userId","userInfoArray[1] as userName","userInfoArray[2] as userScreenName","userInfoArray[3] as userLocation","userInfoArray[4] as userFollowersCount","tweetReTweetCount","tweetFavoriteCount")
    val dateValue=dateFormat.format(Calendar.getInstance().getTime())
    //displaying it
    spark.readStream.format("kafka").option("kafka.bootstrap.servers",KafkaServers).option("subscribe", KafkaTopic).option("startingOffsets","earliest").load.selectExpr(initialColumnsFromStream:_*).selectExpr(tweetColumnsSelectExpr:_*).selectExpr(tweetFinalColumnsSelectExpr:_*).writeStream.format("console").option("checkpointLocation",s"/use/temp/someLocation${dateValue}/").start
    // writing to parquet file
    spark.readStream.format("kafka").option("kafka.bootstrap.servers",KafkaServers).option("subscribe", KafkaTopic).option("startingOffsets","earliest").load.selectExpr(initialColumnsFromStream:_*).selectExpr(tweetColumnsSelectExpr:_*).selectExpr(tweetFinalColumnsSelectExpr:_*).writeStream.format("parquet").option("checkpointLocation",s"/use/temp/someLocation${dateValue}/").start(s"/use/temp/outputLocation${dateValue}")

*/

  }



}
