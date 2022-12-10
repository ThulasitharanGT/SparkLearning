package org.controller.pulsar

import org.apache.pulsar.client.impl.schema.JSONSchema

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

object pulsarSchemaTmpConsumer {

  def main(args: Array[String]): Unit = {
    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650")
      .build

    val pulsarConsumer = pulsarClient.newConsumer(JSONSchema.of(new tmpUserCool().getClass.asInstanceOf[Class[tmpUserCool]]))
      .consumerName("consumerCool")
      .topics(List("cool1", "cool2").asJava).subscriptionName("sub1Cool")
      .subscribe.receive

    println(pulsarConsumer.getKey)
    println(new String(pulsarConsumer.getData, StandardCharsets.UTF_8))
    println(pulsarConsumer.getIndex)
    println(pulsarConsumer.getEventTime)
    println(pulsarConsumer.getReaderSchema)
    println(pulsarConsumer.getMessageId)
    println(pulsarConsumer.getTopicName)

  }

}
