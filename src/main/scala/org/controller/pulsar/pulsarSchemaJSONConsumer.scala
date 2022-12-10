package org.controller.pulsar

import org.apache.pulsar.client.impl.schema.JSONSchema

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

object pulsarSchemaJSONConsumer {

  def main(args: Array[String]): Unit = {
    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650")
      .build

    val pulsarReader = pulsarClient.newReader(
      JSONSchema.of(new tmpUserCool().getClass.asInstanceOf[Class[tmpUserCool]]))
      .topics(List("cool1", "cool2").asJava)
      .startMessageId(org.apache.pulsar.client.api.MessageId.earliest)
      .readerName("readerCool1").subscriptionName("subCool1")
      .create

    while (pulsarReader.hasMessageAvailable) {
      val tmpMessage = pulsarReader.readNext
      println(s"reader data ${new String(tmpMessage.getData, StandardCharsets.UTF_8)}")
      println(s"reader key ${tmpMessage.getKey}")
      println(s"reader getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"reader getEventTime ${tmpMessage.getEventTime}")
      println(s"reader getIndex ${tmpMessage.getIndex}")
      println(s"reader getMessageId ${tmpMessage.getMessageId}")
      println(s"reader getProducerName ${tmpMessage.getProducerName}")
      println(s"reader getSequenceId ${tmpMessage.getSequenceId}")
    }

    pulsarReader.close
    pulsarClient.close

  }

}
