package org.controller.twitterPullTry
import org.constants.projectConstants._

/*
Pulled data from twitter need's to be replicated twice in order to be saved. So using kafka with 2 broker's . creating a topic with replication 2 in order to solve this problem
Once into kafka, we can read through spark steaming.
The problem is twitter tweet's are having multiline and special character's , so using custom delimiter to pass as a message to queue and splitting using custom delimiter to retrieve entire message

Docker config stored in input twitter and spark kafka docker details
 */

import java.util.{Collections, Properties}
import java.util.concurrent.LinkedBlockingQueue
import com.google.gson.Gson
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.{Callback,ProducerRecord,Producer,ProducerConfig,KafkaProducer}
import com.google.common.collect.Lists

object twitterKafka {
  var client:Client = null
  var queue:LinkedBlockingQueue[String]= new LinkedBlockingQueue[String]()
  var gson:Gson =null
  var callback:Callback =null
  val consumerKey="kAlyImDVZ7LQOPDCTnjEI731n"
  val consumerSecret="gIfGV3zeKwf6dhrO1YO2Ta7hfIalCIA2gFXWoI7WynZWdUartK"
  val accessToken="2931317574-jXxy3ftdn7ZzDQKf9NaJrjxSj8owKXsQ9sN4JUm"
  val accessTokenSecret="cagYILh3I5QAWzTnWzXThpyPnvUXbeRZ8LraYVNdrlp6T"
  // val KafkaTopic= "bmw-tweets"
  val KafkaTopic= "bigdata-tweets"
  val KafkaServers="localhost:9092,localhost:9093"
  val tweetDelimiter="~~Tweet-Delimiter~~"
  val userDelimiter="~~User-Delimiter~~"

  def getProducer():Producer[Long, String]={
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KafkaServers)
    properties.put(ProducerConfig.ACKS_CONFIG, "1")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "500")
    properties.put(ProducerConfig.RETRIES_CONFIG, "0")
    properties.put(kafkaKeySerializerArg, "org.apache.kafka.common.serialization.LongSerializer")
    properties.put(kafkaValueSerializerArg, "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer=new KafkaProducer[Long, String](properties)
    kafkaProducer
  }


  def main(args:Array[String]):Unit= {
    val authentication = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    val endpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint
    endpoint.trackTerms(Lists.newArrayList("twitterapi", "#bigdata"))
    //endpoint.trackTerms(Collections.singletonList("#bmw"))
    queue = new LinkedBlockingQueue[String](10000)
    client = new ClientBuilder().hosts(Constants.STREAM_HOST).authentication(authentication).endpoint(endpoint).processor(new StringDelimitedProcessor(queue)).build()
    gson = new Gson()
    callback = new BasicCallback()
    client.connect()
    try  {
      val producer = getProducer()
      while (booleanTrue) {
        val tweet:Tweet = gson.fromJson(queue.take,classOf[Tweet])   //.fromJson(queue.take(), Tweet.class)
        println(s"Fetched tweet id ${tweet.id}")
        println(s"Tweet Msg = ${tweet.text}")
        val key = tweet.id
        //case class Tweet (id:Long,text:String,lang:String,user:User,retweetCount:Int,favoriteCount:Int)
        //val msg = tweet.toString()
        // val msg = s"""{"id":${tweet.id},"text":"${tweet.text}","lang":"${tweet.lang}","user":"${tweet.user}","retweetCount":${tweet.retweetCount},"favoriteCount":${tweet.favoriteCount}}"""
        val msg = s"""${tweet.id}${tweetDelimiter}${tweet.text}${tweetDelimiter}${tweet.lang}${tweetDelimiter}${tweet.user.id}${userDelimiter}${tweet.user.name}${userDelimiter}${tweet.user.screenName}${userDelimiter}${tweet.user.location}${userDelimiter}${tweet.user.followersCount}${tweetDelimiter}${tweet.retweetCount}${tweetDelimiter}${tweet.favoriteCount}"""
        println(s"Kafka Msg = ${msg}") // goes as a single message and is splitted while reading stream
        val record = new ProducerRecord[Long, String](KafkaTopic, key, msg)
        producer.send(record, callback)
      }
    } catch  {
      case e:Exception => e.printStackTrace()
    } finally {
      client.stop()
    }
  }

}
