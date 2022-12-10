package org.controller.pulsar

import org.apache.pulsar.client.api.Schema

object staticSchemaDerived {

  def main(args: Array[String]): Unit = {

    val pulsarUtil = pulsarUtils.pulsarUtil("pulsar://localhost:6650")

    val pulsarClient = pulsarUtil.getPulsarClient


    /*org.apache.pulsar.client.api.PulsarClient
      .builder.serviceUrl("pulsar://localhost:6650")
      .build
*/
    val pulsarProducer = pulsarClient.newProducer(
      Schema.AVRO(new tmpUserCool().getClass.asInstanceOf[Class[tmpUserCool]]))
      .producerName("prd1Try")
      .topic("avroStatic")
      .create

    pulsarProducer.send(new tmpUserCool("bilibili", 4))
    pulsarProducer.close

    val pulsarConsumer = pulsarUtil.getPulsarConsumer[tmpUserCool](
      "consumerTry",
      "avroStatic",
      Some(Schema.AVRO(new tmpUserCool().getClass.asInstanceOf[Class[tmpUserCool]]))
      , "subscriptionTry"
      , org.apache.pulsar.client.api.MessageId.earliest
    )

    pulsarUtil.logMessage[tmpUserCool]("schemaInternal"
      , pulsarConsumer)


    pulsarConsumer.close
    pulsarClient.close

  }
}
