package org.controller.rddCheck

import org.apache.spark.Partition
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.util.SparkOpener
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization._

import java.util.Properties
import java.util.concurrent.ThreadLocalRandom

object rddTmpCheck extends SparkOpener{
  val spark=SparkSessionLoc()
  import spark.implicits._

  val charsList=('A' to 'Z' )++ ('a' to 'z' )
  val charsListSize=charsList.size -1

  def getSingleChar=charsList(ThreadLocalRandom.current.nextInt(0,charsListSize))

  def randomStringGen(sizeOfString:Integer = 5,outputString:String=""):String = sizeOfString match {
    case value if value ==0 => outputString
    case value =>randomStringGen(value-1,s"${outputString}${getSingleChar}")
  }

  def main(args:Array[String]):Unit={
    val tmpArrayBuffer=collection.mutable.ArrayBuffer[String]()
    for (i <- 1 to 100)
      tmpArrayBuffer+=randomStringGen()
    tmpArrayBuffer.toSeq.toDF.rdd.mapPartitions(x=>{ x.map(t => println(s"${t} is in partition ${x.toList.toString}") ) } )
    tmpArrayBuffer.toSeq.toDF.rdd.mapPartitions(_.map(partitionAssigner)).mapPartitions(caseClassUpdaterForDeadLock).groupBy(_.key)
    tmpArrayBuffer.toSeq.toDF.rdd.map(partitionAssigner).map(caseClassUpdaterForDeadLock).groupBy(_.key)

    val props=new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "local");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer= new KafkaProducer[Long,String](props)
    for (i<- 1 to 100)
      kafkaProducer.send(new ProducerRecord[Long,String]("tmpTopic", 0, 1L, randomStringGen()))
    kafkaProducer.close


  }
 case class undoDeadLock(key:String,value:String)

  def caseClassUpdaterForDeadLock(t:Iterator[Row])=t.map(r => undoDeadLock(r(0).toString,r(1).toString))
  def caseClassUpdaterForDeadLock(t:Row)= undoDeadLock(t(0).toString,t(1).toString)

  def partitionAssigner(t:Row):Row =     t(0).toString match {
      case value if value.startsWith("A") || value.startsWith("a") => Row("A", value)
      case value if value.startsWith("B") || value.startsWith("b") => Row("B", value)
      case value if value.startsWith("C") || value.startsWith("c") => Row("C", value)
      case value if value.startsWith("D") || value.startsWith("d") => Row("D", value)
      case value if value.startsWith("E") || value.startsWith("e") => Row("E", value)
      case value if value.startsWith("F") || value.startsWith("f") => Row("F", value)
      case value if value.startsWith("G") || value.startsWith("g") => Row("G", value)
      case value if value.startsWith("H") || value.startsWith("h") => Row("H", value)
      case value if value.startsWith("I") || value.startsWith("i") => Row("I", value)
      case value if value.startsWith("J") || value.startsWith("j") => Row("J", value)
      case value if value.startsWith("K") || value.startsWith("k") => Row("K", value)
      case value if value.startsWith("L") || value.startsWith("l") => Row("L", value)
      case value if value.startsWith("M") || value.startsWith("m") => Row("M", value)
      case value if value.startsWith("N") || value.startsWith("n") => Row("N", value)
      case value if value.startsWith("O") || value.startsWith("o") => Row("O", value)
      case value if value.startsWith("P") || value.startsWith("p") => Row("P", value)
      case value if value.startsWith("Q") || value.startsWith("q") => Row("Q", value)
      case value if value.startsWith("R") || value.startsWith("r") => Row("R", value)
      case value if value.startsWith("S") || value.startsWith("s") => Row("S", value)
      case value if value.startsWith("T") || value.startsWith("t") => Row("T", value)
      case value if value.startsWith("U") || value.startsWith("u") => Row("U", value)
      case value if value.startsWith("V") || value.startsWith("v") => Row("V", value)
      case value if value.startsWith("W") || value.startsWith("w") => Row("W", value)
      case value if value.startsWith("X") || value.startsWith("x") => Row("X", value)
      case value if value.startsWith("Y") || value.startsWith("y") => Row("Y", value)
      case value if value.startsWith("Z") || value.startsWith("z") => Row("Z", value)
    }
}
