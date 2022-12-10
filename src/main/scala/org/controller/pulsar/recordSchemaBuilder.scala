package org.controller.pulsar

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.schema.{GenericRecord, SchemaBuilder}
import org.apache.pulsar.common.schema.SchemaType

object recordSchemaBuilder {

  def main(args: Array[String]): Unit = {
    val pulsarUtil = pulsarUtils.pulsarUtil("pulsar://localhost:6650")

    val pulsarClient = pulsarUtil.getPulsarClient

    val recordSchemaBuilder = SchemaBuilder.record("schemaName")

    recordSchemaBuilder.field("recNumber").`type`(SchemaType.INT32)

    val pulsarProducer = pulsarUtil.getPulsarProducer("prodTmp", "topicGenericRecord"
      , Some(Schema.generic(recordSchemaBuilder.build(SchemaType.AVRO))))
      .asInstanceOf[org.apache.pulsar.client.api.Producer[GenericRecord]]

    pulsarProducer.newMessage.value(Schema.generic(recordSchemaBuilder.build(SchemaType.AVRO))
      .newRecordBuilder
      .set("recNumber", 32)
      .build).send

    pulsarProducer.close

    val pulsarConsumer = pulsarUtil.getPulsarConsumer("consumerTmp", "topicGenericRecord"
      , Some(Schema.generic(recordSchemaBuilder.build(SchemaType.AVRO)))
      , "subsPulsarConsumer", org.apache.pulsar.client.api.MessageId.earliest)

    pulsarUtil.logMessage[GenericRecord]("idTmp", pulsarConsumer)

    pulsarConsumer.close
    pulsarClient.close

  }

}
