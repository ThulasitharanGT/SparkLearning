package org.util.kafka

import org.apache.kafka.common.utils.Time

object checkIfTopicExists {
def main(args:Array[String]):Unit ={
  val inputMap:collection.mutable.Map[String,String]= collection.mutable.Map[String,String]()
  for (arg <- args)
  {
    val keyPart=arg.split("=",2)(0)
    val valuePart=arg.split("=",2)(1)
    inputMap.put(keyPart,valuePart)
  }
  // for zookeeper connection
  val topicName=inputMap("topicName")
  val zookeeperIp=inputMap("zookeeperIp")
  val isSecure=inputMap("isSecure").toBoolean
  val sessionTimeOutMilliSecs=inputMap("sessionTimeOutMilliSecs").toInt
  val connectionTimeOutMilliSecs=inputMap("connectionTimeOutMilliSecs").toInt
  val maxInFlightRequests=inputMap("maxInFlightRequests").toInt
  lazy val time=Time.SYSTEM
  val metricGroup=inputMap("metricGroup")
  val metricType=inputMap("metricType")

  // for zookeeper connection
  /* val topicName="CarSensor"
   val zookeeperIp="localhost:3039"
   val isSecure=false
   val sessionTimeOutMilliSecs=200000
   val connectionTimeOutMilliSecs=15000
   val maxInFlightRequests=20
   lazy val time=Time.SYSTEM
   val metricGroup="myGroup"
   val metricType="myType"
   val sleepMsForKafkaToDeleteTopic=60000
   */
  //getting zk connection for kafka
  val zkKafkaClient=kafkaUtilMethods.getKafkaZookeeperClientConnection(zookeeperIp,isSecure,sessionTimeOutMilliSecs,connectionTimeOutMilliSecs,maxInFlightRequests,time,metricGroup,metricType)
 // val adminZKClientConnection=kafkaUtilMethods.getAdminZookeeperClientConnection(zkKafkaClient) //used for create or delete a topic
  kafkaUtilMethods.checkIfTopicExists(zkKafkaClient,topicName) match { case true => println(topicName+" exists in "+zookeeperIp)  case _ => println(topicName+" does not exist in "+zookeeperIp)}
}
}
