package org.util.kafka
import org.apache.kafka.common.utils.Time
object deleteTopic {
  def main(args: Array[String]): Unit = {
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
    val sleepMsForKafkaToDeleteTopic=inputMap("sleepMsForKafkaToDeleteTopic").toLong

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
    val zkKafkaClient=kafkaUtilMethods.getKafkaZookeeperClientConnection(zookeeperIp,isSecure,sessionTimeOutMilliSecs,connectionTimeOutMilliSecs,maxInFlightRequests,time,metricGroup,metricType)
    val adminZKClientConnection=kafkaUtilMethods.getAdminZookeeperClientConnection(zkKafkaClient)

   // val topicCheckResultBefore=kafkaUtilMethods.checkIfTopicExists(zkKafkaClient,topicName)
    //println(topicCheckResultBefore)
    kafkaUtilMethods.checkIfTopicExists(zkKafkaClient,topicName) match {
      case true =>{ kafkaUtilMethods.deleteTopic(adminZKClientConnection,topicName) // kafka will mark the topic for deletion, and then delete it when it gets refreshed based on time interval or it's internal mechanism. It'll delete the topic after 60000 ms
       // val topicCheckResultAfter=kafkaUtilMethods.checkIfTopicExists(zkKafkaClient,topicName) // if we directly match , some times it wont work
        //println(topicCheckResultAfter)
        println(topicName+" in "+zookeeperIp+" has been " +
          "marked for deletion")
        println("Waiting for cluster to delete the topic.......")
        Thread.sleep(sleepMsForKafkaToDeleteTopic) //sleeping till kafka deletes the topic after it has marked the topic for deletion (60000 ms)
        kafkaUtilMethods.checkIfTopicExists(zkKafkaClient,topicName) match{case false =>println("Topic deleted from cluster") case _ => println("Topic present in cluster")}}
      case _  => println("Topic not present in cluster") }
  }


}
