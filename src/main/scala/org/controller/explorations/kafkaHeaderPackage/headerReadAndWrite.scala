package org.controller.explorations.kafkaHeaderPackage

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.util.SparkOpener
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.spark.sql.{ForeachWriter, Row}

import java.util.concurrent.ThreadLocalRandom

object headerReadAndWrite extends SparkOpener{
  val spark=SparkSessionLoc()
  val strSeq=(('a' to 'z') ++ ('A' to 'Z')).map(_.toString)
  val strSeqSize=strSeq.size -1
  val inputMap=collection.mutable.Map[String,String]()

  def main(args:Array[String]):Unit ={
 for(arg <- args)
   inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    spark.readStream.format("rate").option("rowsPerSecond ","100").load.writeStream.format("console")
      .outputMode("append").option("checkpointLocation",inputMap("checkpointLocation")).foreach(writer = new ForeachWriter[org.apache.spark.sql.Row] {
      override def open(partitionId: Long, epochId: Long): Boolean = true
      override def process(value: Row): Unit = {
        val producer=getProducer
        producer.send(getProducerRecordWithHeaders(inputMap("topic")))
        producer.close
      }
      override def close(errorOrNull: Throwable): Unit = Unit
    }).start

    spark.streams.awaitAnyTermination
}
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
      getIterableOfHeaders(value-1,headerKeySize,headerValSize,tempIterable match {case value if value == null =>getSeqHeader(headerKeySize,headerValSize) case _ => tempIterable++getSeqHeader })
  }

  def getSeqHeader(headerKeySize:Int=5,headerValSize:Int=5)=Seq(getHeaderRecord(headerKeySize,headerValSize))

  def getIterableOfHeadersDiff(numHeaders:Int=5,headerKeySize:Int=5,headerValSize:Int=5,tempIterable:Seq[Header]=Seq.empty):Seq[Header]=numHeaders match {
    case value if value ==0 => tempIterable match { case value if value.isEmpty ==true => getSeqHeader(headerKeySize,headerValSize) case _ =>tempIterable }
    case value if value >0 =>
      getIterableOfHeaders(value-1,headerKeySize,headerValSize,tempIterable match {case value if value.isEmpty == true =>getSeqHeader(headerKeySize,headerValSize) case _ => tempIterable++getSeqHeader })
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
