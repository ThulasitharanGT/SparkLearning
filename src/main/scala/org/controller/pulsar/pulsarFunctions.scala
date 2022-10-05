package org.controller.pulsar



import org.apache.pulsar.functions.api.Context
import org.apache.pulsar.functions.api.Function

class pulsarFunctions extends Function[String,String]{
  override def process(input: String, context: Context) =s"${input}!"
}
