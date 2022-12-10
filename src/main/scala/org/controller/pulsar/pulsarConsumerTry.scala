package org.controller.pulsar

import java.nio.charset.StandardCharsets

object pulsarConsumerTry {

  def main(args: Array[String]): Unit = {

    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650").build

    val reader = pulsarClient.newReader.readerName("reader1")
      .subscriptionName("sub2")
      .topic("cool1")
      .startMessageId(org.apache.pulsar.client.api.MessageId.earliest)
      .create

    while (reader.hasMessageAvailable)
      reader.readNext() match {
        case msg =>
          println(s"reader data ${new String(msg.getData, StandardCharsets.UTF_8)}")
          println(s"reader key ${msg.getKey}")
          println(s"reader getBrokerPublishTime ${msg.getBrokerPublishTime}")
          println(s"reader getEventTime ${msg.getEventTime}")
          println(s"reader getIndex ${msg.getIndex}")
          println(s"reader getMessageId ${msg.getMessageId}")
          println(s"reader getProducerName ${msg.getProducerName}")
          println(s"reader getSequenceId ${msg.getSequenceId}")
      }

  }

}
