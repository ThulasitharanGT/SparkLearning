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
