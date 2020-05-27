package org.controller.twitterPullTry

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
class BasicCallback extends  Callback{
  @Override
  def onCompletion( metadata:RecordMetadata,  exception:Exception) {
    if (exception == null)
      println(s"Message with offset ${metadata.offset()} acknowledged by partition ${metadata.partition()}")
    else
      println(exception.getMessage)
  }
}
