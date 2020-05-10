package temp

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.Level
import org.apache.spark.sql.Row
import org.apache.spark.streaming.twitter._
import org.util.SparkOpener
import twitter4j.TwitterFactory
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
object twitterTemp extends SparkOpener{
val spark=SparkSessionLoc("temp twitter")
val sc=spark.sparkContext
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(sc, Seconds(3600))
    //keys
    val consumerKey="UO5DiXtq33DksC03pUEL9ilz0"
    val consumerSecret="5OlifhEZjPOSiukouZJYxdlY4pVYKx7KtnOO2QU6m7oXQ987sL"
    val accessToken="2931317574-JMMr2oKwdX4yBLmDGhySizkTWDtF1eJfNsPDTX3"
    val accessTokenSecret="vRS7yH3nFgAu0h0U3rKnTfvYVPLZVKiSXNatQultPMfZ9"
    val filters: Seq[String] = Seq("car")
    val cb = new ConfigurationBuilder().setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret).build()
    val twitter_auth = new TwitterFactory(cb)
    val authVariable = new OAuthAuthorization(cb)
    val atwitter: Option[twitter4j.auth.Authorization] = Some(twitter_auth.getInstance(authVariable).getAuthorization())
    val tweets = TwitterUtils.createStream(ssc, atwitter, filters, StorageLevel.MEMORY_AND_DISK_SER_2)
    //val statuses = tweets.map(_.getText)
    val statuses = tweets.map(tweet => (tweet,1))
    val tweetCounts=statuses.reduceByKey(_ + _)
   // val twitterRDD=tweetCounts.map(record=> twitterResult(record(0),record(1))) // does't work
    import spark.implicits._
    ssc.checkpoint("/user/twitter/checkpoint/")
    //statuses.print
    statuses.saveAsTextFiles("tweets", "json")
    ssc.start()
  }

}
