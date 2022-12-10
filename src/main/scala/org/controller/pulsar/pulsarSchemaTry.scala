package org.controller.pulsar

object pulsarSchemaTry {

  def main(args:Array[String]):Unit ={

    val pulsarClient =org.apache.pulsar.client.api.PulsarClient.builder
      .serviceUrl("pulsar://localhost:6650").build

    val pulsarProducer = pulsarClient.newProducer
      .producerName("producer1")
      .topic("cool1")
      .create

    pulsarProducer.send(new tmpUser("coolTopic1",12).parse)

    pulsarProducer.close
    pulsarClient.close

  }

}
class tmpUser(val name:String,val age:Int){
  def parse = s"""{"name":"$name","age":$age}""".getBytes
}



