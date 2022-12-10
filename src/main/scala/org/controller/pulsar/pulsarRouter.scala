package org.controller.pulsar

import org.apache.pulsar.functions.api.{Context, Function}

object pulsarRouter extends Function[String,Option[Throwable]]{

  val fruitTopic="com.fruits"
  val vegetableTopic="com.vegetables"

  val vegetableList="Onion,Tomato,Potato,Okra".split(",")
  val fruitsList="Apple,Orange,Banana,Grapes".split(",")


  import scala.util.{Failure, Success, Try}

  override def process(input: String, context: Context) = input.split("~")  match {
    case value if vegetableList.map(_.equalsIgnoreCase(value.head)).filter(_ == true).size >0 =>
      returner(Try{  writeToTopic("vegetable",value(1))})
    case value if fruitsList.map(_.equalsIgnoreCase(value.head)).filter(_ == true).size >0 =>
      returner(Try{  writeToTopic("fruit",value(1))})
  }


  def returner(statement:Try[Any]) =statement match {
    case Success(value) => None
    case Failure(exception) =>
      exception.printStackTrace
      Some(exception)
  }




  def writeToTopic(controlStr:String,msg:String)=(controlStr.toLowerCase,pulsarUtils.pulsarUtil("pulsar://localhost:6650")) match {
    case ("fruit",pulsarInstance) =>
      sendMessage(msg,pulsarInstance.getPulsarProducer[String]("fruitProd",fruitTopic,Some(org.apache.pulsar.client.api.Schema.STRING)).asInstanceOf[org.apache.pulsar.client.api.Producer[String]])
      pulsarInstance.closePulsarClient()
    case ("vegetable",pulsarInstance) =>
      sendMessage(msg,pulsarInstance.getPulsarProducer[String]("vegetableProd",vegetableTopic,Some(org.apache.pulsar.client.api.Schema.STRING)).asInstanceOf[org.apache.pulsar.client.api.Producer[String]])
      pulsarInstance.closePulsarClient()
  }

  def sendMessage[T <: Any](msg:T,pulsarProducer:org.apache.pulsar.client.api.Producer[ T]) =
    {
      pulsarProducer.send(msg)
      pulsarProducer.close
    }


}
