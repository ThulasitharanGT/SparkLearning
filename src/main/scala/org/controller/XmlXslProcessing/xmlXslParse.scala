package org.controller.XmlXslProcessing

import org.util.SparkOpener
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import java.io.{ByteArrayOutputStream, InputStream, StringReader}
import javax.xml.transform.{Result, Source, Transformer, TransformerFactory}
import javax.xml.transform.stream.{StreamResult, StreamSource}
import org.apache.spark.sql.expressions.Window

// try -D and System.getProperty
object xmlXslParse extends SparkOpener{

  case class layerOneMessage(message:String)
  case class itemInfo(itemID:String,price:Int,quantity:Int,discountApplied:String,discountId:Option[String],discountAmount:Option[Int])
  case class orderInfo(orderID:String,sessionID:String
                       ,userId:String,accountId:String,orderItems:Seq[itemInfo]
                       ,address:String,insOrUpd:String
                       ,incomingTS:java.sql.Timestamp)
  case class baseMsg(verbString:String,message:String)

  def getMsgInfo(eventString:String)=baseMsg(eventString.split("\\|\\|")(0),eventString.split("\\|\\|")(1))

  def clearString(tmpString:String,tmpSeq:Seq[(String,String)]=Seq(("(",")"),("[","]"),(",",","))):String= tmpSeq match {
    case head :: tail =>
      clearString(clearAround(tmpString,head._1,head._2),tail)
    case Nil =>
      tmpString
  }

  def clearAround(tmpString:String,startString:String,endString:String)= tmpString.startsWith(startString) match {
    case value if value ==true =>
      tmpString.drop(1).endsWith(endString) match {
        case value if value == true =>
          tmpString.drop(1).dropRight(1)
        case value if value == false =>
          tmpString.drop(1)
      }
    case value if value ==false =>
      tmpString.endsWith(endString) match {
        case value if value == true =>
          tmpString.dropRight(1)
        case value if value == false =>
          tmpString
      }
  }

  def constructEvent(event:baseMsg)=constructProperEvent(event.verbString,constructMap(event.message))

  def constructMap(msgString:String)=collection.mutable.Map(msgString.split("\\+").flatMap(x => Map(x.split("\\:\\:")(0) -> clearString(x.split("\\:\\:")(1),Seq((",",","),("(",")"),("[","]"))))).toSeq:_*)

  val dateFormat=new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  def constructProperEvent(verb:String,dataMap:collection.mutable.Map[String,String])= verb match {
    case value if value == "INS" =>
      orderInfo(dataMap("orderId"),dataMap("sessionID"),dataMap("userId"),dataMap("accountID"),constructItemEventIns(dataMap("orderItems")),dataMap("Address"),verb,new java.sql.Timestamp(dateFormat.parse(dataMap("incomingTS")).getTime))
    case value if value == "UPD" =>
      orderInfo(dataMap("orderId"),dataMap("sessionID"),dataMap("userId"),dataMap("accountID"),constructItemEventUpd(dataMap("orderItems")),dataMap("Address"),verb,new java.sql.Timestamp(dateFormat.parse(dataMap("incomingTS")).getTime))
  }

  def constructItemEventIns(msgString:String)=msgString.split("\\),\\(").map(x=> x.split("~")).map(x => itemInfo(x(0),x(1).toInt,x(5).toInt,x(2),x(2) match {case value if value =="Y" => Some(x(3)) case _ => None},x(2) match {case value if value =="Y" => Some(x(4).toInt) case _ => None} ) ).toSeq
  def constructItemEventUpd(msgString:String)=msgString.split("\\),\\(").map(x=> x.split("~")).map(x => itemInfo(x(0),x(1).toInt,x(2).toInt,"N",x(3) match {case value if value =="NA" => None case value =>Some(value) },x(3) match {case value if value =="NA" => None case _ =>Some(x(4).toInt) } ) ).toSeq


  val spark =SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  def main(args:Array[String]):Unit={
    val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

   val layerOneEncoder = Encoders.product[layerOneMessage]
   val layerOneExprEncoder = layerOneEncoder.asInstanceOf[ExpressionEncoder[layerOneMessage]]
   val structSchema=layerOneExprEncoder.schema
/*

  spark.readStream.format("kafka").option("subscribe",inputMap("topic"))
      .option("startingOffsets",inputMap("startingOffsets"))
      .option("kafka.bootstrap.servers",inputMap("bootstrapServers")).load
      .select(col("value").cast(StringType).as("message")).as[layerOneMessage]
    .map(x => constructEvent(getMsgInfo(getString(x.message,"/home/raptor/IdeaProjects/SparkLearning/Input/xslParsing/tmpXSL.xsl"))))
    //     .map(x => constructEvent(getMsgInfo(getString(x.message))))
    .withColumn("itemInfoArr",explode(col("orderItems")))
    .drop("orderItems").select(col("orderID")
    ,col("sessionID"),col("userId"),col("accountId")
    ,col("address"),col("insOrUpd"),col("incomingTS"),col("itemInfoArr.*"))
    .withWatermark("incomingTS","5 minutes")
    .withColumn("totalAmount",sum("price").over(Window.partitionBy("orderID")))
    .writeStream.format("console").outputMode("update").option("truncate","false")
    .option("checkpointLocation",inputMap("checkpointLocForStream")).start
  //  .option("checkpointLocation",System.getProperty("checkpointLocForStream")).start
*/


    //  .as[layerOneExprEncoder]

    val inputStream=spark.readStream.format("kafka")
      .option("subscribe",inputMap("topic")).option("startingOffsets",inputMap("startingOffsets"))
      .option("kafka.bootstrap.servers",inputMap("bootstrapServers")).load.select(col("value")
      .cast(StringType).as("message")).as[layerOneMessage]
      .map(x => constructEvent(getMsgInfo(getString(x.message,inputMap("xslFilePath")))))
      .withColumn("itemInfoArr",explode(col("orderItems")))
      .drop("orderItems").select(col("orderID"),col("sessionID")
      ,col("userId"),col("accountId"),col("address"),col("insOrUpd")
      ,col("incomingTS"),col("itemInfoArr.*"))

    val aggDF=inputStream
      .withWatermark("incomingTS",s"${System.getProperty("delayThreshold")}")
      .groupBy(window($"incomingTS", s"${System.getProperty("windowDuration")}",
        s"${System.getProperty("slideDuration")}"), $"orderID")
      .agg(sum("price").as("totalPrice"))
      .selectExpr("orderID","totalPrice","window.start as window_start","window.end as window_end")


    val finalDF=inputStream.join(aggDF, Seq("orderID"))

    spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled","false")

    finalDF.writeStream.format("console").outputMode("append")
      .option("truncate","false").option("checkpointLocation",inputMap("checkpointLocForStream")).start

spark.streams.awaitAnyTermination

  }

  def getString(str:String,xslFilePath:String)={
    val newInputSource= new java.io.FileInputStream(xslFilePath)
    val outputStream = new ByteArrayOutputStream
    val result = new StreamResult(outputStream)
    val xmlSource = new StreamSource(new StringReader(str))
    val xslSource = new StreamSource(newInputSource)
    val transformer = TransformerFactory.newInstance.newTransformer(xslSource)
    transformer.transform(xmlSource, result)
    outputStream.toString
  }
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --class org.controller.XmlXslProcessing.xmlXslParse --num-executors 2 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 2 --driver-java-options "-DslideDuration=\"4 minutes\" -DdelayThreshold=\"3 minutes\" -DwindowDuration=\"4 minutes\"" /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar topic="xml.input" startingOffsets="latest" bootstrapServers="localhost:8082,localhost:8083,localhost:8084" checkpointLocForStream="hdfs://localhost:8020/user/raptor/stream/checkpointPointLoc"
  def getString(str:String)={
    val newInputSource= getClass.getResourceAsStream("./tmpXSL.xsl")
    val outputStream = new ByteArrayOutputStream
    val result = new StreamResult(outputStream)
    val xmlSource = new StreamSource(new StringReader(str))
    val xslSource = new StreamSource(newInputSource)
    val transformer = TransformerFactory.newInstance.newTransformer(xslSource)
    transformer.transform(xmlSource, result)
    outputStream.toString
  }
}
