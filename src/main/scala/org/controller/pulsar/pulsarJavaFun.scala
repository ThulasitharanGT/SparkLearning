package org.controller.pulsar

class pulsarJavaFun extends java.util.function.Function[String,String] {
  override def apply(input: String) = s"${input}!"
}
