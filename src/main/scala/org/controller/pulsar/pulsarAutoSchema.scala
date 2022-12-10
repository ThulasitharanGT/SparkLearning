package org.controller.pulsar

import org.apache.pulsar.client.api.Schema

object pulsarAutoSchema {

  def main(args: Array[String]): Unit = {
    val pulsarUtil = pulsarUtils.pulsarUtil("pulsar://localhost:6650")

    val pulsarClient = pulsarUtil.getPulsarClient

    val pulsarProducer = pulsarUtil.getPulsarProducer[Array[Byte]]("prodTmp", "autoSchema"
      , Some(Schema.AUTO_PRODUCE_BYTES))

    pulsarProducer.send("""{"payload":"{\"data\":\"insert\",\"type\":\"cool\"}","msgType":"msgInfo"}""".getBytes)

    pulsarProducer.close

    val pulsarConsumer = pulsarUtil.getPulsarConsumer[org.apache.pulsar.client.api.schema.GenericRecord]("consumerTmp"
      , "autoSchema", Some(Schema.AUTO_CONSUME)
      , "subscriptionName"
      , org.apache.pulsar.client.api.MessageId.earliest)

    pulsarUtil.logMessage[org.apache.pulsar.client.api.schema.GenericRecord]("autoSchema", pulsarConsumer)

    pulsarClient.close


  }

}
