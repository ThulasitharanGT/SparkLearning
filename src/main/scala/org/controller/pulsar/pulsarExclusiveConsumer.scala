package org.controller.pulsar

import org.apache.pulsar.client.api.{PulsarClient, Schema, SubscriptionType}

//  spark-submit --master local[*] --packages org.apache.pulsar:pulsar-client:2.8.2 --class org.controller.pulsar.pulsarExclusiveConsumer --driver-memory 512m /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar
class pulsarExclusiveConsumer {

  def main(args:Array[String]):Unit={
    val client=PulsarClient.builder.serviceUrl("pulsar://localhost:6650").build
    val consumer=client.newConsumer(Schema.STRING)
      .topic("exclusiveTopic")
      .subscriptionName("exclusiveSubscription1")
      .subscriptionType(SubscriptionType.Exclusive).subscribe

    while (!consumer.hasReachedEndOfTopic)
    consumer.receive match {
      case messageOfString =>
        println(new String(messageOfString.getData))
    }
  }

}
