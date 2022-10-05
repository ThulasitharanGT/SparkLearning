package org.controller.pulsar

import java.nio.charset.StandardCharsets

object pulsarUtils {

  case class pulsarUtil(serviceUrl: String) {
    var pulsarClient: org.apache.pulsar.client.api.PulsarClient = null

    val getPulsarClient =
      org.apache.pulsar.client.api.PulsarClient.builder.serviceUrl(this.serviceUrl).build
      match {
        case value: org.apache.pulsar.client.api.PulsarClient =>
          pulsarClient = value
          value
      }


    val closePulsarClient =() => try {
      this.pulsarClient.close
    }
    catch {
      case npe: NullPointerException =>
        println("client is not open yet")
      case e: Exception =>
        println("Error while closing client")
        println(e.getMessage)
        e.printStackTrace
    }

    def getPulsarProducer[T](producerName: String, topic: String
                             , schema: Option[org.apache.pulsar.client.api.Schema[T]]) =
      schema match {
        case Some(schema) =>
          this.pulsarClient.newProducer(schema).topic(topic).producerName(producerName).create.asInstanceOf[org.apache.pulsar.client.api.Producer[T]]
        case None =>
          this.pulsarClient.newProducer.topic(topic).producerName(producerName).create.asInstanceOf[org.apache.pulsar.client.api.Producer[T]]
      }

    //     org.apache.pulsar.client.api.MessageId.fromByteArray("messageID".getBytes)

    def getPulsarConsumer[T](consumerName: String, topic: String
                             , schema: Option[org.apache.pulsar.client.api.Schema[T]]
                             , subscriptionName: String
                             , startMessageId: org.apache.pulsar.client.api.MessageId) =
      schema match {
        case Some(schema) =>
          this.pulsarClient.newReader(schema).topic(topic).readerName(consumerName)
            .subscriptionName(subscriptionName).startMessageId(startMessageId).create
        case None =>
          this.pulsarClient.newReader.topic(topic).readerName(consumerName)
            .subscriptionName(subscriptionName).startMessageId(startMessageId).create
      }

    def logMessage[T](identificationString: String
                      , pulsarReader: org.apache.pulsar.client.api.Reader[_ >: T with Array[Byte]]) =
      while (pulsarReader.hasMessageAvailable)
        pulsarReader.readNext match {
          case tmpMessage =>
            println(s"${identificationString} reader data ${new String(tmpMessage.getData, StandardCharsets.UTF_8)}")
            println(s"${identificationString} reader key ${tmpMessage.getKey}")
            println(s"${identificationString} reader getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
            println(s"${identificationString} reader getEventTime ${tmpMessage.getEventTime}")
            println(s"${identificationString} reader getIndex ${tmpMessage.getIndex}")
            println(s"${identificationString} reader getMessageId ${tmpMessage.getMessageId}")
            println(s"${identificationString} reader getProducerName ${tmpMessage.getProducerName}")
            println(s"${identificationString} reader getSequenceId ${tmpMessage.getSequenceId}")
        }
  }


}