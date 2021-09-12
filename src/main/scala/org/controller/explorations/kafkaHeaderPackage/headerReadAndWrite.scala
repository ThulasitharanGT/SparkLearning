package org.controller.explorations.kafkaHeaderPackage

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.util.SparkOpener
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.spark.sql.{ForeachWriter, Row}

import java.text.SimpleDateFormat
import java.util.concurrent.ThreadLocalRandom
import sys.process._
import scala.util.{Try,Success,Failure}
object headerReadAndWrite extends SparkOpener{
  val spark=SparkSessionLoc()
  val strSeq=(('a' to 'z') ++ ('A' to 'Z')).map(_.toString)
  val strSeqSize=strSeq.size -1
  val inputMap=collection.mutable.Map[String,String]()

  def clearCheckpoint(checkpointLocation:String)=Try{s"hdfs dfs -rm -r ${checkpointLocation}"!} match {
    case Success(s) =>commandSuccessEvaluator(s)
    case Failure(f) => println(s"Error occurred while clearing checkpointLocation ${checkpointLocation} ${f.printStackTrace} with exception message ${f.getMessage}")
  }
  def commandSuccessEvaluator(exitCode:Int)=exitCode match { case value if value ==0 => println(s"checkpointLocation was cleared successfully") case value if value !=0 =>println(s"checkpointLocation didn't exist or some other error has occurred, please check the logs")}

  def main(args:Array[String]):Unit ={
 for(arg <- args)
   inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    clearCheckpoint(inputMap("checkpointLocationForEach"))

    spark.readStream.format("rate").option("rowsPerSecond ","100").load.writeStream.format("console")
      .outputMode("append").option("checkpointLocation",inputMap("checkpointLocationForEach")).foreach(writer = new ForeachWriter[org.apache.spark.sql.Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = true
      override def process(value: Row): Unit = {
        val producer=getProducer
        producer.send(getProducerRecordWithHeaders(inputMap("topic")))
        producer.close
      }
      override def close(errorOrNull: Throwable): Unit = Unit
    }).start
/*

spark-submit --class org.controller.explorations.kafkaHeaderPackage.headerReadAndWrite --packages org.apache.kafka:kafka-clients:2.8.0 --num-executors 2 --executor-memory 1g --executor-cores 2 --driver-cores 4 --driver-memory 1g --conf spark.sql.shuffle.partitions=4 --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1000m /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootStrapServers="localhost:8083,localhost:8084,localhost:8082" topic=tmpTopic keySerializers="org.apache.kafka.common.serialization.StringSerializer" groupId="tmp_group_local" valueSerializers="org.apache.kafka.common.serialization.StringSerializer" checkpointLocationForEach="hdfs://localhost:/user/raptor/kafka/check1"
spark-submit --class org.controller.explorations.kafkaHeaderPackage.headerReadAndWrite --packages org.apache.kafka:kafka-clients:2.8.0 --num-executors 2 --executor-memory 1g --executor-cores 2 --driver-cores 4 --driver-memory 1g --conf spark.sql.shuffle.partitions=4 --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1000m /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootStrapServers="localhost:8083,localhost:8084,localhost:8082" topic=tmpTopic  keySerializers="org.apache.kafka.common.serialization.StringSerializer" groupId="tmp_group_local" valueSerializers="org.apache.kafka.common.serialization.StringSerializer" checkpointLocationEachBatch="hdfs://localhost:/user/raptor/kafka/check2"


    spark.readStream.format("rate").option("rowsPerSecond ","100").load.writeStream.format("console")
      .outputMode("append").option("checkpointLocation",inputMap("checkpointLocationEachBatch")).foreachBatch(
      (currentMicroBatch:org.apache.spark.sql.DataFrame,batchID:Long) => forEachBatchFun(currentMicroBatch,batchID)

    ).start
*/

    spark.streams.awaitAnyTermination
}
  case class dataRate(timeStamp:java.sql.Timestamp,dataNum:Int)
  def forEachBatchFun(df:org.apache.spark.sql.DataFrame,batchID:Long)={
    import spark.implicits._
    println(s"batchID ${batchID}")
    df.map(x=> {
      val producer=getProducer
      producer.send(getProducerRecordWithHeaders(inputMap("topic"))).get
      producer.close
      dataRate(dateToTimeStamp(x(0).toString),x(1).toString.toInt)}).show(false)
  }
  val sparkTimeStampFormat= new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
  def dateToTimeStamp(dateStr:String)= new java.sql.Timestamp(sparkTimeStampFormat.parse(dateStr).getTime)


  def getProps= new java.util.Properties
  def getKafkaProps={
    val props=getProps
    props.put("bootstrap.servers",inputMap("bootStrapServers"))
    props.put("key.serializer",inputMap("keySerializers"))
    props.put("group.id",inputMap("groupId"))
    props.put("value.serializer",inputMap("valueSerializers"))
    props
  }

  def getProducer=new KafkaProducer[String,String](getKafkaProps)

  def getSingleString=strSeq(getRandomNum)
  def getRandomNum=ThreadLocalRandom.current.nextInt(0,strSeqSize)

  def getHeaderRecord(headerKeySize:Int=5,headerValSize:Int=6):Header= new RecordHeader(getRandomString(headerKeySize),getByteArray(headerValSize))

  def getIterableOfHeaders(numHeaders:Int=5,headerKeySize:Int=5,headerValSize:Int=5,tempIterable:Seq[Header]=null):Seq[Header]=numHeaders match {
    case value if value ==0 => tempIterable match { case value if value ==null => getSeqHeader(headerKeySize,headerValSize) case _ =>tempIterable }
    case value if value >0 =>
      getIterableOfHeaders(value-1,headerKeySize,headerValSize,tempIterable match {case value if value == null =>getSeqHeader(headerKeySize,headerValSize) case _ => tempIterable++getSeqHeader() })
  }

  def getSeqHeader(headerKeySize:Int=5,headerValSize:Int=5)=Seq(getHeaderRecord(headerKeySize,headerValSize))

  def getIterableOfHeadersDiff(numHeaders:Int=5,headerKeySize:Int=5,headerValSize:Int=5,tempIterable:Seq[Header]=Seq.empty):Seq[Header]=numHeaders match {
    case value if value ==0 => tempIterable match { case value if value.isEmpty ==true => getSeqHeader(headerKeySize,headerValSize) case _ =>tempIterable }
    case value if value >0 =>
      getIterableOfHeaders(value-1,headerKeySize,headerValSize,tempIterable match {case value if value.isEmpty == true =>getSeqHeader(headerKeySize,headerValSize) case _ => tempIterable++getSeqHeader() })
  }

  def getHeaderRecords(numHeaders:Int=5,headerKeySize:Int=5,headerValSize:Int=5):java.lang.Iterable[Header]= new RecordHeaders(getIterableOfHeaders(numHeaders,headerKeySize,headerValSize).toArray)

  def getRandomString(strSize:Int=5,tmpString:String=""):String=strSize match {
    case value if value ==0 => tmpString
    case value if value >0 =>   getRandomString(value-1 ,s"${tmpString}${getSingleString}")
  }

  def getByteArray(strSize:Int=5)=getRandomString(strSize).toArray.map(_.toByte)

  def getProducerRecordWithHeaders(topic:String,numHeaders:Int=5,headerKeySize:Int=5,headerValSize:Int=6,msgKeySize:Int=4,msgValSize:Int=10)=new ProducerRecord[String,String](topic,getRandomPartitionNumForTopic(topic),getRandomString(msgKeySize),getRandomString(msgValSize),getHeaderRecords(numHeaders,headerKeySize,headerValSize))
  def getProducerRecordWithHeader(topic:String,headerKeySize:Int=5,headerValSize:Int=6,msgKeySize:Int=4,msgValSize:Int=10)= new ProducerRecord[String,String](topic,getRandomPartitionNumForTopic(topic),getRandomString(msgKeySize),getRandomString(msgValSize)).headers.add(getHeaderRecord(headerKeySize,headerValSize))

  def getRandomPartitionNumForTopic(topic:String)= partitionArrayLastIndex(getNumPartitionsOfTopic(topic)) match {case value if value==1 => 0 case value if value>1 => ThreadLocalRandom.current.nextInt(0,value)}

  def partitionArrayLastIndex(sizeOfPartitions:Int)= sizeOfPartitions-1
  def getNumPartitionsOfTopic(topic:String)={
      val producer=getProducer
      val partitions=getNumPartitions(producer,topic)
      producer.close
      partitions
    }
  def getNumPartitions(producer:KafkaProducer[String,String],topic:String)=producer.partitionsFor(topic).size

}
