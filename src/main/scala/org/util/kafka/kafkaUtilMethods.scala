package org.util.kafka
import java.util.Properties
import kafka.admin.RackAwareMode
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.utils.Time

object kafkaUtilMethods {
  def getKafkaZookeeperClientConnection(zookeeperIp:String, isSecure:Boolean, sessionTimeOutMilliSecs:Int, connectionTimeOutMilliSecs:Int, maxInFlightRequests:Int, time:Time, metricGroup:String, metricType:String):KafkaZkClient=KafkaZkClient.apply(zookeeperIp,isSecure,sessionTimeOutMilliSecs,connectionTimeOutMilliSecs,maxInFlightRequests,time,metricGroup,metricType)
  def getAdminZookeeperClientConnection(zkKafkaClient:KafkaZkClient):AdminZkClient=new AdminZkClient(zkKafkaClient)
  def createTopicWithDetails(adminZKClientConnection:AdminZkClient,topicName:String,topicPartitions:Int,topicReplicationFactor:Int,topicConfig:Properties)=adminZKClientConnection.createTopic(topicName,topicPartitions,topicReplicationFactor,topicConfig,RackAwareMode.Disabled)
  def getTopicProperties(adminZKClientConnection:AdminZkClient,topicName:String)=adminZKClientConnection.getAllTopicConfigs().get(topicName) // gives an option [Some] means topic exists option [none] means topic does not exists
  def checkIfTopicExists(zkKafkaClient:KafkaZkClient,topicName:String)=zkKafkaClient.topicExists(topicName) // true if  exists and false if topic does not exists
  def getAllTopic(zkKafkaClient:KafkaZkClient) = zkKafkaClient.getAllTopicsInCluster
  def deleteTopic(adminZKClientConnection:AdminZkClient,topicName:String)=adminZKClientConnection.deleteTopic(topicName)

}

/*
spark-submit --class org.util.kafka.createTopic  --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 512m --driver-cores 1 --packages org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.12:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  topicName=msgCheck topicPartitions=10 topicReplicationFactor=2 zookeeperIp=localhost:3039 isSecure=false sessionTimeOutMilliSecs=200000 connectionTimeOutMilliSecs=15000 maxInFlightRequests=20 metricGroup=myGroup metricType=myType

spark-submit --class com.test.AtomicityUsingMysql.randomMsgProducer --driver-memory 512m --executor-memory 1g --num-executors 1 --executor-cores 2 --driver-cores 1 --packages org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.12:2.3.1 /home/raptor/IdeaProjects/KafkaGradleTest/build/libs/KafkaGradleTest-1.0-SNAPSHOT-all.jar key=check messageLength=5 numOfRecords=50 keySerializer=org.apache.kafka.common.serialization.StringSerializer valueSerializer=org.apache.kafka.common.serialization.StringSerializer bootStrapServer=localhost:9091,localhost:9092,localhost:9093 topicName=msgCheck


spark-submit --class org.util.kafka.deleteTopic  --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 512m --driver-cores 1 --packages org.apache.kafka:kafka-clients:2.3.1,org.apache.kafka:kafka_2.12:2.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar  topicName=msgCheck topicPartitions=10 topicReplicationFactor=2 zookeeperIp=localhost:3039 isSecure=false sessionTimeOutMilliSecs=200000 connectionTimeOutMilliSecs=15000 maxInFlightRequests=20 metricGroup=myGroup metricType=myType sleepMsForKafkaToDeleteTopic=60000

 */
