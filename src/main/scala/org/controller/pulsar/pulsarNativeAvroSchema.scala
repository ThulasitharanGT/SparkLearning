package org.controller.pulsar

import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.commons.io.output.ByteArrayOutputStream

object pulsarNativeAvroSchema {

  def main(args: Array[String]): Unit = {

    val pulsarUtil = pulsarUtils.pulsarUtil("pulsar://localhost:6650")

    val avroSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.RECORD)

    val empRec= emp.newBuilder().setName("coool").setAge(21).setId(1).setSalary(2345).setAddress("dsfddfd").build

    val writer = new SpecificDatumWriter[emp](avroSchema)
    val os = new ByteArrayOutputStream

    val encoder = EncoderFactory.get.binaryEncoder(os, null)
    writer.write(empRec, encoder)
    encoder.flush();

    val avroBytes = os.toByteArray()

    val pulsarProducer = pulsarUtil.getPulsarProducer[Array[Byte]]("producerNameAvro"
      , "avroTopic",
      Some(org.apache.pulsar.client.api.Schema.BYTES))

    pulsarProducer.newMessage(org.apache.pulsar.client.api.Schema.NATIVE_AVRO(avroSchema))
      .value(avroBytes).send

    pulsarProducer.close

    pulsarUtil.closePulsarClient()

  }

}
