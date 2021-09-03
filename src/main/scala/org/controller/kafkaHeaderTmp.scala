package org.controller

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader,RecordHeaders}
import java.util.{Arrays,Properties}

object kafkaHeaderTmp {
  def main (args:Array[String]):Unit ={
    val props=new Properties
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers","localhost:8082,localhost:8083,localhost:8084")

    val kafkaProducer=new KafkaProducer[String,String](props)
    val header= new RecordHeader("str1","tho".getBytes)
    val headers:Iterable[Header]= Iterable(new RecordHeader("str1","tho".getBytes),new RecordHeader("str2","th1".getBytes))
    val headersArray:java.lang.Iterable[Header]=Arrays.asList(new RecordHeader("str1","tho".getBytes),new RecordHeader("str2","th1".getBytes))

    // below uses scala iterator. We nned to use java Iterator as kafka is written in java
   // val headersArrays:Iterable[Header]=Arrays.asList(new RecordHeader("str1","tho".getBytes),new RecordHeader("str2","th1".getBytes))


    val producerRecord= new ProducerRecord[String,String]("tmpTopic",0,"","",headersArray)

    kafkaProducer.send(producerRecord)

    // 2nd way to add

    val producerRecord2= new ProducerRecord[String,String]("tmpTopic",0,"das","tyu")
    producerRecord2.headers.add(new RecordHeader("srt","str".getBytes()))
    producerRecord2.headers.add(new RecordHeader("sr2","st3".getBytes()))
    kafkaProducer.send(producerRecord2)

    // third way
    val recHeaders= new RecordHeaders(headersArray) // .asInstanceOf[Iterable[Header]]
    val producerRecord3= new ProducerRecord[String,String]("tmpTopic",0,"ytr","trdc",recHeaders)
    kafkaProducer.send(producerRecord3)

    kafkaProducer.close
  }
}
