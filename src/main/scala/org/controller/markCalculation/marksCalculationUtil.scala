package org.controller.markCalculation

import org.apache.spark.sql.streaming.DataStreamWriter
import org.controller.markCalculation.marksCalculationConstant._
import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.databind.{DeserializationFeature,ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


object marksCalculationUtil {

  def inputArrayToMap(args:Array[String])={
    val inputMap=collection.mutable.Map[String,String]()
    for(arg <- args)
      addElementToMap(arg,inputMap)
    inputMap
  }

  def getSparkSession(conf:org.apache.spark.SparkConf=null)= conf match {
    case null => org.apache.spark.sql.SparkSession.builder.enableHiveSupport.getOrCreate
    case value => org.apache.spark.sql.SparkSession.builder.enableHiveSupport.config(value).getOrCreate
  }


  def addElementToMap(arg:String,tmpMap:collection.mutable.Map[String,String])= splitArg(arg) match {
   case value if value.size ==2 =>
     tmpMap.put(keyGetter(value) ,valGetter(value))
     tmpMap
   case _ =>tmpMap
 }

  val getReadStreamDF:(org.apache.spark.sql.SparkSession,collection.mutable.Map[String,String])=> org.apache.spark.sql.DataFrame = (spark:org.apache.spark.sql.SparkSession,inputMap:collection.mutable.Map[String,String]) =>
 Try{inputMap(readStreamFormat)} match {
   case Success(s)=>
     println(s"getReadStreamDF :: Success")
     s match {
     case value if value == deltaStreamFormat =>
       println(s"getReadStreamDF :: Success delta")
       spark.readStream.format("delta").load(inputMap(pathArg))
     case value if value == kafkaStreamFormat =>
       println(s"getReadStreamDF :: Success kafka")
      // println(s"getReadStreamDF :: Success kafka :: kafkaStreamFormat ${getSubscribeAssignValue(inputMap)}")
      // println(s"getReadStreamDF :: Success kafka :: kafkaSubscribeAssignDecider ${subscribeAssignDecider(inputMap(kafkaSubscribeAssignDecider))}")
       spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers",inputMap(kafkaBootstrapServerArg))
         .option(subscribeAssignDecider(inputMap(kafkaSubscribeAssignDecider)),getSubscribeAssignValue(inputMap))
         .option("startingOffsets",inputMap(kafkaStartingOffsetsArg))
         .load
   }
   case Failure(f)=> // reads parquet
     println(s"getReadStreamDF :: Failure")
     spark.readStream.load(inputMap(pathArg))
 }

  val getSubscribeAssignValue:(collection.mutable.Map[String,String])=>String=(map:collection.mutable.Map[String,String])=> Try{map(kafkaSubscribeAssignDecider)} match {
    case Success(s) =>
      s match {case value if value ==kafkaSubscribe => map(kafkaSubscribe) case value if value ==kafkaAssign => map(kafkaAssign)}
    case Failure(f) =>""
  }

  def subscribeAssignDecider(deciderString:String)=deciderString match {
    case value if value == kafkaSubscribe => "subscribe"
    case value if value == kafkaAssign => "assign"
  }


  val dfWriterStream:(org.apache.spark.sql.SparkSession,org.apache.spark.sql.DataFrame,collection.mutable.Map[String,String])=>Unit=
    (spark:org.apache.spark.sql.SparkSession,df:org.apache.spark.sql.DataFrame,tmpMap:collection.mutable.Map[String,String]) => Try{tmpMap(writeStreamFormat)} match {
    case Success(s) =>
      println("dfWriterStream Success")
      s match {
      case value if value == deltaStreamFormat =>
        println("dfWriterStream Success delta")
        tmpMap(deltaMergeOverwriteDecider) match {
          case value if value == deltaMerge =>
            println("dfWriterStream Success delta merge")
            df.write.format("delta").mode("append").option("mergeSchema",deltaMergeOverwriteHelper(tmpMap,deltaMerge)).save(tmpMap("path"))
          case value if value == deltaOverwrite =>
            println("dfWriterStream Success delta overwrite")
            df.write.format("delta").mode("append").option("overwriteSchema",deltaMergeOverwriteHelper(tmpMap,deltaOverwrite)).save(tmpMap("path"))
          case value if value == deltaMergeAndOverwrite =>
            println("dfWriterStream Success delta merge and overwrite")
            df.write.format("delta").mode("append").option("mergeSchema",deltaMergeOverwriteHelper(tmpMap,deltaMerge)).option("overwriteSchema",deltaMergeOverwriteHelper(tmpMap,deltaOverwrite)).save(tmpMap("path"))
        }
      case value if value == kafkaStreamFormat =>
        println("dfWriterStream Success kafka")
        import spark.implicits._
        df.map(x=> kafkaWrapper(x)).map(toJson).writeStream.format("kafka").option("kafka.bootstrap.servers",tmpMap(kafkaBootstrapServerArg)).option("topic",tmpMap(kafkaTopic)).option("checkpointLocation",tmpMap(checkpointLocation)).start
    }
    case Failure(f) =>
      println("dfWriterStream Failure")
      df.write.mode("append").save(tmpMap("path"))
    }

  def deltaMergeOverwriteHelper(map:collection.mutable.Map[String,String],getStr:String) =  Try{map(getStr)} match { case Success(s) => s case Failure(f) => "false"}

  def keyGetter(argSplit:Array[String])=  Try{argSplit(0)} match {case Success(s) => s case Failure(f) => ""}

  val valGetter:(Array[String])=> String = (argSplit:Array[String]) =>  Try{argSplit(1)} match {case Success(s) => s case Failure(f) => ""}
  val splitArg:(String)=>Array[String] = (arg:String)=> Try{arg.split("=",2)} match {case Success(s) => s case Failure(f) => Array.empty}


  val mapper=new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false)

  def toJson(value:Any)= mapper.writeValueAsString(value)
  def fromJson[T](json:String)(implicit m:Manifest[T])=mapper.readValue[T](json)


}

case class kafkaWrapper(value:Any)


case class tmpD(cool:String,cool2:String)

object tmpD {
def apply(cool:String,cool2:String):tmpD = tmpD(cool,cool2)
}
