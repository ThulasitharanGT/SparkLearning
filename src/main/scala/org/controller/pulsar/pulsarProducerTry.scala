package org.controller.pulsar

object pulsarProducerTry {
  def main(args: Array[String]): Unit = {

    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650")
      .build

    val pulsarProducer = pulsarClient.newProducer.producerName("prod1")
      .topic("cool1")
      .create

    pulsarProducer.send("message".getBytes)

    pulsarProducer.close
    pulsarClient.close


  }

}
