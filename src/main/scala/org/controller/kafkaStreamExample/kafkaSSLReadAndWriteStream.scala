package org.controller.kafkaStreamExample

import java.nio.file.{Files, Path,Paths}

object kafkaSSLReadAndWriteStream {
def main(args:Array[String])={
  val filesTemp=Files.getLastModifiedTime(Paths.get("/home/raptor/Softwares/kafka_2.12-2.7.0/keys/client.keystore.jks"))
  val filesTempMillis=Files.getLastModifiedTime(Paths.get("/home/raptor/Softwares/kafka_2.12-2.7.0/keys/client.keystore.jks")).toMillis

}
}
