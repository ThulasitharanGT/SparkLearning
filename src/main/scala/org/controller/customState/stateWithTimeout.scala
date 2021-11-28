package org.controller.customState

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.util.SparkOpener

object stateWithTimeout extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  def groupByFun(tempClass:tmpStruct)=tempClass.key
  case class tmpStruct(key:String,value:dataClass)
  case class dataClass(messageReceived:String)
  case class dataClassWithKey(key:String="",messagesReceived:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]("" -> ""))
  case class wrapperCaseClass(dataMap:collection.mutable.Map[String,String])
// custom state
  case class dataClassWithTimeStamp(key:String,value:String,incomingTimestamp:java.sql.Timestamp=new java.sql.Timestamp(System.currentTimeMillis))

  // timeout by spark
  def mapGroupFunctionWithTimeOut(key:String,incomingEvents:Iterator[tmpStruct],stateObj:GroupState[wrapperCaseClass]):dataClassWithKey ={

    def checkState(key:String,incomingEventsInner:Iterator[tmpStruct],stateObj:GroupState[wrapperCaseClass]) ={
      stateObj.getOption match {
        case Some(state) =>
          println(s"Some State ")
          state.dataMap.filter(_._1.startsWith(key)).size >0 match {
            case true =>
              println(s"Data present in state ")
           //   val tmpDataMap=collection.mutable.Map[String,String]()
           //   val eventsList=incomingEventsInner.toList
         //     eventsList.map(x => tmpDataMap.put(s"${key}_${System.currentTimeMillis}",x.value.messageReceived))
              stateObj.update(wrapperCaseClass(state.dataMap ++tmpListToMapAppender(key,incomingEventsInner.toList) ))
              stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
              dataClassWithKey(key,stateObj.get.dataMap)
            case false =>
              println(s"Data not present in state ")
              stateObj.update(wrapperCaseClass(/*state.dataMap ++*/tmpListToMapAppender(key,incomingEventsInner.toList) ))
              stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
              dataClassWithKey()
          }
        case None =>
          println(s"None State ")
          //    val tmpDataMap=collection.mutable.Map[String,String]()
       //   val eventsList=incomingEventsInner.toList
     //     eventsList.map(x => tmpDataMap.put(s"${key}_${System.currentTimeMillis}",x.value.messageReceived))
          stateObj.update(wrapperCaseClass(tmpListToMapAppender(key,incomingEventsInner.toList)))
          stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
          dataClassWithKey()
      }
    }

    stateObj.hasTimedOut match {
      case value if value == true =>
        println(s"State has timed out ")
        stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
        stateObj.update(wrapperCaseClass(/*stateObj.get.dataMap.filter(! _._1.startsWith(key)) ++*/ tmpListToMapAppender(key,incomingEvents.toList)))
        checkState(key, incomingEvents, stateObj)
      case value if value== false =>
        println(s"State has not timed out ")
        checkState(key, incomingEvents, stateObj)
    }
  }

  def tmpListToMapAppender(key:String,tmpList:List[tmpStruct])={
    val tmpMap=collection.mutable.Map[String,String]()
    tmpList.map(x => tmpMap.put(s"${key}_${System.currentTimeMillis}",x.value.messageReceived))
    tmpMap
  }
// apply your logic here , Remove from map if its past a certain time
  // timeout by me

  def mapGroupFunctionWithCustomTimeOut(key:String,incomingEvents:Iterator[dataClassWithTimeStamp],stateObj:GroupState[wrapperCaseClass]) ={
    stateObj.getOption match {
      case  Some(state)=>
      case None =>
    }
  }

  val inputMap=collection.mutable.Map[String,String]()

  def main(args:Array[String]):Unit ={
    import spark.implicits._

    args.map(_.split("=",2)).map(x => inputMap.put(x(0),x(1)))
/*data will be retained in state for 5 mins, after that cleared,
Funda is, message needs to be sent more than once with same key in order to be released.
If timed out, message again needs to be sent with same key twice, if state has a message for that key then everything will be released

  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --class org.controller.customState.stateWithTimeout --num-executors 1 --executor-memory 1g --driver-memory 1g --driver-cores 1 --executor-cores 1 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootStrapServers=localhost:8082,localhost:8083,localhost:8084 topic=tmpTopic checkpointLocation="hdfs://localhost:8020/user/raptor/stream/state1/" ExpiryDuration="5 minutes"
  */
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",inputMap("bootStrapServers"))
      .option("subscribe",inputMap("topic")).load.select(
      org.apache.spark.sql.functions.split(
        org.apache.spark.sql.functions.col("value")
          .cast(org.apache.spark.sql.types.StringType)
          .as("valueCasted"),",").as("valueSplitted"))
      .selectExpr("valueSplitted[0] as key","valueSplitted[1] as value" ).map(x => tmpStruct(x.getAs[String]("key"),dataClass(x.getAs[String]("value"))))
      .groupByKey(groupByFun).mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mapGroupFunctionWithTimeOut) // check event timeout with watermark
      // .mapGroupsWithState(GroupStateTimeout.NoTimeout)(mapGroupFunction) // curried function
      .withColumn("cool",org.apache.spark.sql.functions.lit("cool"))
      .writeStream.format("console").outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start

    spark.streams.awaitAnyTermination
  }
}
