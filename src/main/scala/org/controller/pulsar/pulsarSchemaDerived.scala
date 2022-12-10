package org.controller.pulsar

import org.apache.pulsar.client.api.{Message, Schema}
import org.apache.pulsar.common.schema.{KeyValue, KeyValueEncodingType}

import java.nio.charset.StandardCharsets

object pulsarSchemaDerived {

  def main(args: Array[String]): Unit = {

    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650")
      .build

    val pulsarProducerKeyValInline = pulsarClient.newProducer(Schema.KeyValue(
      Schema.INT32,
      Schema.STRING,
      KeyValueEncodingType.INLINE
    )).topic("ksTopic.inline")
      .create

    pulsarProducerKeyValInline.send(new KeyValue(3, "konegzegg"))
    pulsarProducerKeyValInline.close

    val pulsarConsumerKeyValInline = pulsarClient.newReader(Schema.KeyValue(
      Schema.INT32,
      Schema.STRING,
      KeyValueEncodingType.INLINE
    )).topic("ksTopic.inline")
      .subscriptionName("inlineSubscribe1")
      .startMessageId(org.apache.pulsar.client.api.MessageId.earliest)
      .readerName("inlineReader1")
      .create

    // parsing key along with message is problematic if one is not a byte or a string

    while (pulsarConsumerKeyValInline.hasMessageAvailable)
      pulsarConsumerKeyValInline.readNext match {
        case value: Message[KeyValue[Integer, String]] =>
          println(s"reader data ${new String(value.getData, StandardCharsets.UTF_8)}")
          println(s"reader key ${value.getKey}")
          println(s"reader getBrokerPublishTime ${value.getBrokerPublishTime}")
          println(s"reader getEventTime ${value.getEventTime}")
          println(s"reader getIndex ${value.getIndex}")
          println(s"reader getMessageId ${value.getMessageId}")
          println(s"reader getProducerName ${value.getProducerName}")
          println(s"reader getSequenceId ${value.getSequenceId}")
      }

    pulsarConsumerKeyValInline.close
    // int is producing a string value in key. No Idea why
    val pulsarProducerKeyValSeparate = pulsarClient.newProducer(Schema.KeyValue(
      Schema.STRING,
      Schema.STRING,
      KeyValueEncodingType.SEPARATED
    )).topic("ksTopic.separated.1")
      .create

    pulsarProducerKeyValSeparate.send(new KeyValue("cool", "sfsd"))

    pulsarProducerKeyValSeparate.close

    val pulsarConsumerKeyValSeparate = pulsarClient.newReader(Schema.KeyValue(
      Schema.STRING,
      Schema.STRING,
      KeyValueEncodingType.SEPARATED
    )).subscriptionName("separateSubscription1")
      .topic("ksTopic.separated.1")
      .readerName("separateReader1")
      .startMessageId(org.apache.pulsar.client.api.MessageId.earliest)
      .create

    while (pulsarConsumerKeyValSeparate.hasMessageAvailable)
      pulsarConsumerKeyValSeparate.readNext match {
        case value: Message[KeyValue[String, String]] =>
          println(s"reader data ${new String(value.getData, StandardCharsets.UTF_8)}")
          println(s"reader key ${value.getKey}")
          println(s"reader getBrokerPublishTime ${value.getBrokerPublishTime}")
          println(s"reader getEventTime ${value.getEventTime}")
          println(s"reader getIndex ${value.getIndex}")
          println(s"reader getMessageId ${value.getMessageId}")
          println(s"reader getProducerName ${value.getProducerName}")
          println(s"reader getSequenceId ${value.getSequenceId}")
      }

    pulsarConsumerKeyValSeparate.close
    pulsarClient.close


  }
}
