package org.util.kafka

import java.util.Properties
import org.apache.kafka.common.utils.Time

object createTopic {

  def main(args:Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]= collection.mutable.Map[String,String]()
    for (arg <- args)
    {
      val keyPart=arg.split("=",2)(0)
      val valuePart=arg.split("=",2)(1)
      inputMap.put(keyPart,valuePart)
    }
    // for zookeeper connection
    val zookeeperIp=inputMap("zookeeperIp")
    val isSecure=inputMap("isSecure").toBoolean
    val sessionTimeOutMilliSecs=inputMap("sessionTimeOutMilliSecs").toInt
    val connectionTimeOutMilliSecs=inputMap("connectionTimeOutMilliSecs").toInt
    val maxInFlightRequests=inputMap("maxInFlightRequests").toInt
    lazy val time=Time.SYSTEM
    val metricGroup=inputMap("metricGroup")
    val metricType=inputMap("metricType")
    /*
    val zookeeperIp="localhost:3039"
    val isSecure=false
    val sessionTimeOutMilliSecs=200000
    val connectionTimeOutMilliSecs=15000
    val maxInFlightRequests=20
    lazy val time=Time.SYSTEM
    val metricGroup="myGroup"
    val metricType="myType"
     */
    val zkKafkaClient=kafkaUtilMethods.getKafkaZookeeperClientConnection(zookeeperIp,isSecure,sessionTimeOutMilliSecs,connectionTimeOutMilliSecs,maxInFlightRequests,time,metricGroup,metricType)
    println("Got Zookeeper Kafka Client Connection")
    val adminZKClientConnection=kafkaUtilMethods.getAdminZookeeperClientConnection(zkKafkaClient)
    println("Got Zookeeper admin Client Connection")
    //topic details
    val topicName=inputMap("topicName")
    val topicPartitions=inputMap("topicPartitions").toInt
    val topicReplicationFactor=inputMap("topicReplicationFactor").toInt
    /*
    val topicName="CarSensor"
    val topicPartitions=10
    val topicReplicationFactor=3
     */
    val topicConfig:Properties=new Properties()

    /*
    adminZKClientConnection.createTopic(topicName,topicPartitions,topicReplicationFactor,topicConfig,RackAwareMode.Disabled)

     */
    kafkaUtilMethods.createTopicWithDetails(adminZKClientConnection,topicName,topicPartitions,topicReplicationFactor,topicConfig)
    kafkaUtilMethods.checkIfTopicExists(zkKafkaClient,topicName) match {case true => println("Topic created in cluster"); case _ =>  println("Topic not created in cluster") }
    kafkaUtilMethods.getTopicProperties(adminZKClientConnection,topicName)
    val allTopicInClusterSeq=kafkaUtilMethods.getAllTopic(zkKafkaClient)
    for (topic <- allTopicInClusterSeq)
      println(topic)
  }
}
