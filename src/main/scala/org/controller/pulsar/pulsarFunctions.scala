package org.controller.pulsar

import org.apache.pulsar.functions.api.{Context, Function}

object pulsarFunctions extends Function[String,String]{
  override def process(input: String, context: Context) =s"${input}!"
}
