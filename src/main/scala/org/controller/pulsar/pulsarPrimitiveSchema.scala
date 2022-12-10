package org.controller.pulsar

import org.apache.pulsar.client.api.Schema

import java.nio.charset.StandardCharsets

object pulsarPrimitiveSchema {

  def main(args: Array[String]): Unit = {

    val pulsarClient = org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650").build

    val pulsarProducerStr = pulsarClient.newProducer(Schema.STRING)
      .producerName("stringProd")
      .topic("stringTopic")
      .create

    pulsarProducerStr.send("str")

    val pulsarProducerBoolean = pulsarClient.newProducer(Schema.BOOL).producerName(s"booleanProd").topic(s"topicBoolean").create
    val pulsarProducerByte = pulsarClient.newProducer(Schema.INT8).producerName(s"byteProd").topic(s"topicByte").create
    val pulsarProducerShort = pulsarClient.newProducer(Schema.INT16).producerName(s"shortProd").topic(s"topicShort").create
    val pulsarProducerInt = pulsarClient.newProducer(Schema.INT32).producerName(s"intProd").topic(s"topicInt").create
    val pulsarProducerLong = pulsarClient.newProducer(Schema.INT64).producerName(s"longProd").topic(s"topicLong").create
    val pulsarProducerFloat = pulsarClient.newProducer(Schema.FLOAT).producerName(s"floatProd").topic(s"topicFloat").create
    val pulsarProducerDouble = pulsarClient.newProducer(Schema.DOUBLE).producerName(s"doubleProd").topic(s"topicDouble").create
    val pulsarProducerBytes = pulsarClient.newProducer(Schema.BYTES).producerName(s"byteProd").topic(s"topicBytes").create
    val pulsarProducerString = pulsarClient.newProducer(Schema.STRING).producerName(s"stringProd").topic(s"topicString").create
    val pulsarProducerJavaSqlTimestamp = pulsarClient.newProducer(Schema.TIMESTAMP).producerName(s"java.sql.TimestampProd").topic(s"topicJava.sql.Timestamp").create
    val pulsarProducerJavaSqlTime = pulsarClient.newProducer(Schema.TIME).producerName(s"java.sql.TimeProd").topic(s"topicJava.sql.Time").create
    val pulsarProducerJavaUtilDate = pulsarClient.newProducer(Schema.DATE).producerName(s"java.util.DateProd").topic(s"topicJava.util.Date").create
    val pulsarProducerJavaTimeInstant = pulsarClient.newProducer(Schema.INSTANT).producerName(s"java.time.InstantProd").topic(s"topicJava.time.Instant").create
    val pulsarProducerJavaTimeLocalDate = pulsarClient.newProducer(Schema.LOCAL_DATE).producerName(s"java.time.LocalDateProd").topic(s"topicJava.time.LocalDate").create
    val pulsarProducerJavaTimeLocalDateTime = pulsarClient.newProducer(Schema.LOCAL_DATE_TIME).producerName(s"java.time.LocalDateTimeProd").topic(s"topicJava.time.LocalDateTime").create
    val pulsarProducerJavaTimeLocalTime = pulsarClient.newProducer(Schema.LOCAL_TIME).producerName(s"java.time.LocalTimeProd").topic(s"topicJava.time.LocalTime").create

    pulsarProducerBoolean.send(false)
    pulsarProducerByte.send("a".getBytes.head)
    pulsarProducerShort.send(10.toShort)
    pulsarProducerInt.send(40)
    pulsarProducerLong.send(56)
    pulsarProducerFloat.send(56.7.toFloat)
    pulsarProducerDouble.send(45.7)
    pulsarProducerBytes.send("cool".getBytes)
    pulsarProducerString.send("yuop")
    pulsarProducerJavaSqlTimestamp.send(new java.sql.Timestamp(System.currentTimeMillis))
    pulsarProducerJavaSqlTime.send(new java.sql.Time(System.currentTimeMillis))
    pulsarProducerJavaUtilDate.send(new java.sql.Date(System.currentTimeMillis))
    pulsarProducerJavaTimeInstant.send(java.time.Instant.now)
    pulsarProducerJavaTimeLocalDate.send(java.time.LocalDate.now)
    pulsarProducerJavaTimeLocalDateTime.send(java.time.LocalDateTime.now)
    pulsarProducerJavaTimeLocalTime.send(java.time.LocalTime.now)

    println("done producing")


    val simpleDateFormatInstant = new java.text.SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'")
    val simpleDateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    /*

        "bool" match {
          case value if value.toLowerCase == "boolean" => false
          case value if value.toLowerCase.startsWith("int") =>
            value match {
              case value if value.toLowerCase.endsWith("8") => "s".getBytes.head
              case value if value.toLowerCase.endsWith("16")  => 10.toShort
              case value if value.toLowerCase.endsWith("32")  => 40
              case value if value.toLowerCase.endsWith("64")  => 56L
        }
          case value if value.toLowerCase == "float" => 56.7.toFloat
          case value if value.toLowerCase == "double" => 45.7
          case value if value.toLowerCase == "bytes" => "vool".getBytes.deep
          case value if value.toLowerCase == "string" => "yuop"
          case value if value.toLowerCase == "timestamp" => new java.sql.Timestamp(System.currentTimeMillis)
          case value if value.toLowerCase == "time" => new java.sql.Time(System.currentTimeMillis)
          case value if value.toLowerCase == "date" => new java.sql.Date(System.currentTimeMillis)
          case value if value.toLowerCase == "instant" =>
            java.time.Instant.parse(simpleDateFormatInstant.format(simpleDateFormat.parse(
              new java.sql.Timestamp(System.currentTimeMillis).toString)))
          case value if value.toLowerCase == "local_date" =>
             java.time.LocalDate.parse(new java.sql.Date(System.currentTimeMillis).toString)
          case value if value.toLowerCase == "local_time" =>
             java.time.LocalTime.parse(new java.sql.Time(System.currentTimeMillis).toString)
          case value if value.toLowerCase == "local_date_time" =>
             java.time.LocalDateTime.now
        }

        java.time.Instant.parse(simpleDateFormatInstant.format(simpleDateFormat.parse(
          new java.sql.Timestamp(System.currentTimeMillis).toString))) //"1000-09-09T11:12:13.123Z")
    */

    pulsarProducerStr.close
    pulsarProducerBoolean.close
    pulsarProducerByte.close
    pulsarProducerShort.close
    pulsarProducerInt.close
    pulsarProducerLong.close
    pulsarProducerFloat.close
    pulsarProducerDouble.close
    pulsarProducerBytes.close
    pulsarProducerString.close
    pulsarProducerJavaSqlTimestamp.close
    pulsarProducerJavaSqlTime.close
    pulsarProducerJavaUtilDate.close
    pulsarProducerJavaTimeInstant.close
    pulsarProducerJavaTimeLocalDate.close
    pulsarProducerJavaTimeLocalDateTime.close
    pulsarProducerJavaTimeLocalTime.close


    def getStringFromBytes(input: Array[Byte]) = input.map(_.toString).mkString

    def getDataFromByteArray(data: Array[Byte], src: String) = src match {
      case value if value == "bool" =>
        java.lang.Boolean.valueOf(getStringFromBytes(data))
      case "byte" =>
        java.lang.Byte.valueOf(getStringFromBytes(data)).toChar
      case "short" =>
        data.map(_.shortValue).map(_.toChar)
      case "string" =>
        new String(data, StandardCharsets.UTF_8)
      case "int" =>
        data.map(_.intValue).map(_.toChar)
      case "long" =>
        data.map(_.longValue).map(_.toChar)
      case "float" =>
        data.map(_.floatValue).map(_.toChar)
      case "double" =>
        data.map(_.doubleValue).map(_.toChar)
      case "bytes" =>
        getStringFromBytes(data).map(x => java.lang.Byte.valueOf(x.toString)).map(_.toChar)
      case value if value.contains("timestamp") =>
        new java.sql.Timestamp(simpleDateFormat.parse(new String(data, StandardCharsets.UTF_8)).getTime)
      case value if value.contains("sql.date") =>
        new java.sql.Date(simpleDateFormat.parse(new String(data, StandardCharsets.UTF_8)).getTime)
      case value if value.contains("sql.time") =>
        new java.sql.Time(simpleDateFormat.parse(new String(data, StandardCharsets.UTF_8)).getTime)
      case value if value.contains("util.date") =>
        simpleDateFormat.parse(new String(data, StandardCharsets.UTF_8))
      case value if value.contains("time.instant") =>
        java.time.Instant.parse(new String(data, StandardCharsets.UTF_8))
      case value if value.contains("time.date") =>
        java.time.LocalDate.parse(new String(data, StandardCharsets.UTF_8))
      case value if value.contains("time.local.datetime") =>
        java.time.LocalDateTime.parse(new String(data, StandardCharsets.UTF_8))
      case value if value.contains("time.local.time") =>
        java.time.LocalTime.parse(new String(data, StandardCharsets.UTF_8))
    }

    // except for bolean and string it's very problematic to parse others. Just use json format and send it as string

    val pulsarConsumerBoolean = pulsarClient.newReader(Schema.BOOL).topic("topicBoolean").subscriptionName(s"subscriptionPulsarConsumerBoolean").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerBoolean.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerBoolean.readNext
      println(s"pulsarConsumerBoolean data ${getDataFromByteArray(tmpMessage.getData, "bool")}")
      println(s"pulsarConsumerBoolean key ${tmpMessage.getKey}")
      println(s"pulsarConsumerBoolean getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerBoolean getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerBoolean getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerBoolean getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerBoolean getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerBoolean getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerByte = pulsarClient.newReader(Schema.INT8).topic("topicByte").subscriptionName(s"subscriptionPulsarConsumerByte").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerByte.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerByte.readNext
      println(s"pulsarConsumerByte data ${getDataFromByteArray(tmpMessage.getData, "byte")}")
      println(s"pulsarConsumerByte key ${tmpMessage.getKey}")
      println(s"pulsarConsumerByte getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerByte getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerByte getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerByte getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerByte getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerByte getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerShort = pulsarClient.newReader(Schema.INT16).topic("topicShort").subscriptionName(s"subscriptionPulsarConsumerShort").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerShort.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerShort.readNext
      println(s"pulsarConsumerShort data ${getDataFromByteArray(tmpMessage.getData, "short")}")
      println(s"pulsarConsumerShort key ${tmpMessage.getKey}")
      println(s"pulsarConsumerShort getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerShort getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerShort getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerShort getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerShort getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerShort getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerInt = pulsarClient.newReader(Schema.INT32).topic("topicInt").subscriptionName(s"subscriptionPulsarConsumerInt").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerInt.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerInt.readNext
      println(s"pulsarConsumerInt data ${getDataFromByteArray(tmpMessage.getData, "int")}")
      println(s"pulsarConsumerInt key ${tmpMessage.getKey}")
      println(s"pulsarConsumerInt getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerInt getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerInt getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerInt getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerInt getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerInt getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerLong = pulsarClient.newReader(Schema.INT64).topic("topicLong").subscriptionName(s"subscriptionPulsarConsumerLong").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerLong.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerLong.readNext
      println(s"pulsarConsumerLong data ${getDataFromByteArray(tmpMessage.getData, "long")}")
      println(s"pulsarConsumerLong key ${tmpMessage.getKey}")
      println(s"pulsarConsumerLong getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerLong getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerLong getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerLong getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerLong getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerLong getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerFloat = pulsarClient.newReader(Schema.FLOAT).topic("topicFloat").subscriptionName(s"subscriptionPulsarConsumerFloat").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerFloat.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerFloat.readNext
      println(s"pulsarConsumerFloat data ${getDataFromByteArray(tmpMessage.getData, "float")}")
      println(s"pulsarConsumerFloat key ${tmpMessage.getKey}")
      println(s"pulsarConsumerFloat getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerFloat getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerFloat getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerFloat getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerFloat getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerFloat getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerDouble = pulsarClient.newReader(Schema.DOUBLE).topic("topicDouble").subscriptionName(s"subscriptionPulsarConsumerDouble").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerDouble.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerDouble.readNext
      println(s"pulsarConsumerDouble data ${getDataFromByteArray(tmpMessage.getData, "double")}")
      println(s"pulsarConsumerDouble key ${tmpMessage.getKey}")
      println(s"pulsarConsumerDouble getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerDouble getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerDouble getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerDouble getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerDouble getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerDouble getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerBytes = pulsarClient.newReader(Schema.BYTES).topic("topicBytes").subscriptionName(s"subscriptionPulsarConsumerBytes").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerBytes.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerBytes.readNext
      println(s"pulsarConsumerBytes data ${getDataFromByteArray(tmpMessage.getData, "bytes")}")
      println(s"pulsarConsumerBytes key ${tmpMessage.getKey}")
      println(s"pulsarConsumerBytes getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerBytes getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerBytes getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerBytes getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerBytes getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerBytes getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerString = pulsarClient.newReader(Schema.STRING).topic("topicString").subscriptionName(s"subscriptionPulsarConsumerString").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerString.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerString.readNext
      println(s"pulsarConsumerString data ${getDataFromByteArray(tmpMessage.getData, "string")}")
      println(s"pulsarConsumerString key ${tmpMessage.getKey}")
      println(s"pulsarConsumerString getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerString getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerString getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerString getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerString getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerString getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaSqlTimestamp = pulsarClient.newReader(Schema.TIMESTAMP).topic("topicJava.sql.Timestamp").subscriptionName(s"subscriptionPulsarConsumerJavaSqlTimestamp").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaSqlTimestamp.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaSqlTimestamp.readNext
      println(s"pulsarConsumerJavaSqlTimestamp data ${getDataFromByteArray(tmpMessage.getData, "timestamp")}")
      println(s"pulsarConsumerJavaSqlTimestamp key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaSqlTimestamp getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaSqlTimestamp getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaSqlTimestamp getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaSqlTimestamp getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaSqlTimestamp getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaSqlTimestamp getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaSqlTime = pulsarClient.newReader(Schema.TIME).topic("topicJava.sql.Time").subscriptionName(s"subscriptionPulsarConsumerJavaSqlTime").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaSqlTime.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaSqlTime.readNext
      println(s"pulsarConsumerJavaSqlTime data ${getDataFromByteArray(tmpMessage.getData, "sql.time")}")
      println(s"pulsarConsumerJavaSqlTime key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaSqlTime getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaSqlTime getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaSqlTime getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaSqlTime getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaSqlTime getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaSqlTime getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaUtilDate = pulsarClient.newReader(Schema.DATE).topic("topicJava.util.Date").subscriptionName(s"subscriptionPulsarConsumerJavaUtilDate").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaUtilDate.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaUtilDate.readNext
      println(s"pulsarConsumerJavaUtilDate data ${getDataFromByteArray(tmpMessage.getData, "util.date")}")
      println(s"pulsarConsumerJavaUtilDate key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaUtilDate getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaUtilDate getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaUtilDate getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaUtilDate getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaUtilDate getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaUtilDate getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaTimeInstant = pulsarClient.newReader(Schema.INSTANT).topic("topicJava.time.Instant").subscriptionName(s"subscriptionPulsarConsumerJavaTimeInstant").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaTimeInstant.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaTimeInstant.readNext
      println(s"pulsarConsumerJavaTimeInstant data ${getDataFromByteArray(tmpMessage.getData, "time.instant")}")
      println(s"pulsarConsumerJavaTimeInstant key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaTimeInstant getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaTimeInstant getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaTimeInstant getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaTimeInstant getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaTimeInstant getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaTimeInstant getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaTimeLocalDate = pulsarClient.newReader(Schema.LOCAL_DATE).topic("topicJava.time.LocalDate").subscriptionName(s"subscriptionPulsarConsumerJavaTimeLocalDate").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaTimeLocalDate.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaTimeLocalDate.readNext
      println(s"pulsarConsumerJavaTimeLocalDate data ${getDataFromByteArray(tmpMessage.getData, "time.date")}")
      println(s"pulsarConsumerJavaTimeLocalDate key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaTimeLocalDate getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaTimeLocalDate getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaTimeLocalDate getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaTimeLocalDate getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaTimeLocalDate getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaTimeLocalDate getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaTimeLocalDateTime = pulsarClient.newReader(Schema.LOCAL_DATE_TIME).topic("topicJava.time.LocalDateTime").subscriptionName(s"subscriptionPulsarConsumerJavaTimeLocalDateTime").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaTimeLocalDateTime.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaTimeLocalDateTime.readNext
      println(s"pulsarConsumerJavaTimeLocalDateTime data ${getDataFromByteArray(tmpMessage.getData, "time.local.datetime")}")
      println(s"pulsarConsumerJavaTimeLocalDateTime key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaTimeLocalDateTime getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaTimeLocalDateTime getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaTimeLocalDateTime getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaTimeLocalDateTime getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaTimeLocalDateTime getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaTimeLocalDateTime getSequenceId ${tmpMessage.getSequenceId}")
    }
    val pulsarConsumerJavaTimeLocalTime = pulsarClient.newReader(Schema.LOCAL_TIME).topic("topicJava.time.LocalTime").subscriptionName(s"subscriptionPulsarConsumerJavaTimeLocalTime").startMessageId(org.apache.pulsar.client.api.MessageId.earliest).create

    while (pulsarConsumerJavaTimeLocalTime.hasMessageAvailable) {
      val tmpMessage = pulsarConsumerJavaTimeLocalTime.readNext
      println(s"pulsarConsumerJavaTimeLocalTime data ${getDataFromByteArray(tmpMessage.getData, "time.local.time")}")
      println(s"pulsarConsumerJavaTimeLocalTime key ${tmpMessage.getKey}")
      println(s"pulsarConsumerJavaTimeLocalTime getBrokerPublishTime ${tmpMessage.getBrokerPublishTime}")
      println(s"pulsarConsumerJavaTimeLocalTime getEventTime ${tmpMessage.getEventTime}")
      println(s"pulsarConsumerJavaTimeLocalTime getIndex ${tmpMessage.getIndex}")
      println(s"pulsarConsumerJavaTimeLocalTime getMessageId ${tmpMessage.getMessageId}")
      println(s"pulsarConsumerJavaTimeLocalTime getProducerName ${tmpMessage.getProducerName}")
      println(s"pulsarConsumerJavaTimeLocalTime getSequenceId ${tmpMessage.getSequenceId}")
    }


    pulsarConsumerBoolean.close
    pulsarConsumerByte.close
    pulsarConsumerShort.close
    pulsarConsumerInt.close
    pulsarConsumerLong.close
    pulsarConsumerFloat.close
    pulsarConsumerDouble.close
    pulsarConsumerBytes.close
    pulsarConsumerString.close
    pulsarConsumerJavaSqlTimestamp.close
    pulsarConsumerJavaSqlTime.close
    pulsarConsumerJavaUtilDate.close
    pulsarConsumerJavaTimeInstant.close
    pulsarConsumerJavaTimeLocalDate.close
    pulsarConsumerJavaTimeLocalDateTime.close
    pulsarConsumerJavaTimeLocalTime.close

    pulsarClient.close

  }

}
