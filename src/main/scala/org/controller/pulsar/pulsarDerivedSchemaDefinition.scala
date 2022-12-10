package org.controller.pulsar

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.schema.SchemaDefinition

object pulsarDerivedSchemaDefinition {

  def main(args: Array[String]): Unit = {

    val pulsarUtil = pulsarUtils.pulsarUtil("pulsar://localhost:6650")

    val pulsarClient = pulsarUtil.getPulsarClient

    val userSchemaDef = SchemaDefinition.builder[userClass]
      .withPojo(new userClass().getClass.asInstanceOf[Class[userClass]]).build

    val pulsarProducer = pulsarUtil.getPulsarProducer[userClass]("prodDerived", "derivedTopic",
      Some(Schema.AVRO(userSchemaDef))).asInstanceOf[org.apache.pulsar.client.api.Producer[userClass]]

    pulsarProducer.newMessage.value(userClass.builder.name("pulsar-user").age(1).build)
      .send

    pulsarProducer.close

    val pulsarConsumer = pulsarUtil.getPulsarConsumer[userClass]("consumerDerived", "derivedTopic",
      Some(Schema.AVRO(userSchemaDef)), "subscriptionDerived"
      , org.apache.pulsar.client.api.MessageId.earliest)

    pulsarUtil.logMessage[userClass]("derived", pulsarConsumer)

    pulsarClient.close


  }
}
