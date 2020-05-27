package org.controller.twitterPullTry

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.{Calendar, Date, Properties}
import java.text.SimpleDateFormat
import java.sql.Timestamp
import org.util.SparkOpener

object twitterDataPullFromKafka extends SparkOpener{

  val KafkaTopic= "bigdata-tweets"
  val KafkaServers="localhost:9092,localhost:9093"
  val tweetDelimiter="~~Tweet-Delimiter~~"
  val userDelimiter="~~User-Delimiter~~"
  val schema = new StructType().add("id",LongType).add("text",StringType).add("lang",StringType).add("user",StringType).add("retweetCount",IntegerType)add("favoriteCount",IntegerType)
  val dateFormat = new SimpleDateFormat("YYYYMMddHHmmSS")
  val spark=SparkSessionLoc("temp")

  def main(args:Array[String]):Unit={


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



  }



}
