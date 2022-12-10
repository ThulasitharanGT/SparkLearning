package org.controller.pulsar

import org.apache.pulsar.client.impl.schema.JSONSchema

object pulsarSchemaJSON {

  def main(args: Array[String]): Unit = {
    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650").build

    val pulsarProducer = pulsarClient
      .newProducer(JSONSchema.of(new tmpUserCool().getClass.asInstanceOf[Class[tmpUserCool]]))
      .producerName("producer2")
      .topic("cool2")
      .create

    pulsarProducer.send(new tmpUserCool("coolTopic1", 12))
    pulsarProducer.newMessage.value(new tmpUserCool("coolTopic2", 22)).key("kulkulkool").send

    pulsarProducer.close
    pulsarClient.close
  }
}
