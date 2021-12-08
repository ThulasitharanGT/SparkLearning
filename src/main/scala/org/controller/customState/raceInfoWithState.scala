package org.controller.customState

import org.controller.customState.customStateConstants._
import org.util.SparkOpener
import net.liftweb.json._
import entitiesCustomState._
import org.controller.customState.raceInfoStateUtils._
import scala.util.{Try,Success,Failure}

object raceInfoWithState extends SparkOpener{

  val spark=SparkSessionLoc()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val inputMap=collection.mutable.Map[String,String]()

  def main(args:Array[String]):Unit={
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    inputMap.foreach(println)
    println(s"url - ${inputMap("JDBCUrl")}${inputMap("JDBCDatabase")}?user=${inputMap("JDBCUser")}&password=${inputMap("JDBCPassword")}")

    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",inputMap("bootStrapServers"))
      .option("subscribe",inputMap("topic"))
      .option("startingOffsets",inputMap("startingOffsets"))
      .load.select(org.apache.spark.sql.functions.from_json
      (org.apache.spark.sql.functions.col("value")
      .cast(org.apache.spark.sql.types.StringType)
      ,outerStruct).as("message"))
      .select("message.*")
      .select(org.apache.spark.sql.functions.col("eventInfo")
        .cast(org.apache.spark.sql.types.StringType)
      ,org.apache.spark.sql.functions.col("incomingMessage")
          .cast(org.apache.spark.sql.types.StringType)
      ,org.apache.spark.sql.functions.col("incomingTimestamp")
          .cast(org.apache.spark.sql.types.TimestampType))
      .as[outerSchema].groupByKey(groupByKeyFun)
      .mapGroupsWithState(org.apache.spark.sql.streaming.GroupStateTimeout.NoTimeout)(stateFunction)
      .writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start

    spark.streams.awaitAnyTermination


  }
  implicit val formats = DefaultFormats

  // use no timeout
  // if you use processing timeout, it runs like RDD
  def stateFunction(key:String,incomingEvents:Iterator[outerSchema],groupState:org.apache.spark.sql.streaming.GroupState[stateClass]):stateOutClass=
    groupState.getOption match {
      case Some(state) =>
        state.dataMap.size match {
          case value if value ==0 =>    // no data in state map
            val incomingEventList= incomingEvents.toList
            val parentEvents=incomingEventList.filter(_.eventInfo==raceTrackEventSource)
            val childEvents=incomingEventList.filter(_.eventInfo==raceInfoEventSource)
            val timedOutParentIncomingEvents=stateOutClass(parentEvents.filter(_.hasTimedOut(inputMap("ExpiryDuration"))))
            val validParentIncomingEvents=stateOutClass(parentEvents.filterNot(_.hasTimedOut(inputMap("ExpiryDuration"))))
            val timedOutChildIncomingEvents=stateOutClass(childEvents.filter(_.hasTimedOut(inputMap("ExpiryDuration"))))
            val validChildIncomingEvents=stateOutClass(childEvents.filterNot(_.hasTimedOut(inputMap("ExpiryDuration"))))
            println(s"Some State Data map 0 timedOutParentIncomingEvents ${timedOutParentIncomingEvents}")
            println(s"Some State Data map 0 timedOutChildIncomingEvents ${timedOutChildIncomingEvents}")
            (validParentIncomingEvents.dataList.size,validChildIncomingEvents.dataList.size)   match {
               case (0,0)  => // no parent
                state.dataMap.empty
                groupState.update(state)
                stateOutClass()
               case (x,0) if x >0 => // parent , no child
                state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */ )
                groupState.update(state)
                validParentIncomingEvents
               case (0,x) if x >0 =>  // no parent , child
              //  checkIfRaceTrackExists(parse(validChildIncomingEvents.dataList.head.incomingMessage).extract[raceInfo]) match {
                 checkIfRaceTrackExists(validChildIncomingEvents.dataList.head.getRaceInfoRecord) match {
                  case value if value == validChildIncomingEvents.dataList.head.getRaceInfoRecord.raceTrackID =>
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}",List.empty) */ )
                    groupState.update(state)
                    validChildIncomingEvents
                  case _ =>
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */ )
                    groupState.update(state)
                    stateOutClass()
                }
              case (x,y) if x >0 && y >0 => // parent and child
                validParentIncomingEvents.dataList.head.getKey == validChildIncomingEvents.dataList.head.getKey match {
                  case value if value == true => // get latest and update here
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}_child",List.empty) */ )
                    state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}_parent",List.empty) */ )
                    groupState.update(state)
                    stateOutClass(validChildIncomingEvents.dataList ++ validParentIncomingEvents.dataList)
                  case value if value == false => // just for logic sake
                    checkIfRaceTrackExists(validChildIncomingEvents.dataList.head.getRaceInfoRecord) match {
                      case value if value == validChildIncomingEvents.dataList.head.getRaceInfoRecord.raceTrackID =>
                        state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}",List.empty) */ )
                        state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}",List.empty) */ )
                        groupState.update(state)
                        stateOutClass(validChildIncomingEvents.dataList ++ validParentIncomingEvents.dataList)
                      case _ =>
                        state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */)
                        state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}",List.empty) */ )
                        groupState.update(state)
                       stateOutClass()
                    }
                }
            }
          case value if value > 0 => //data present in stateMap
            val parentStateEvents=state.dataMap.filter(_._1.endsWith("_parent")).map(_._2).toList
            val childStateEvents=state.dataMap.filter(_._1.endsWith("_child")).map(_._2).toList
            val validParentStateEvents=parentStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false))
            val validChildStateEvents=childStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false))
            val timedOutParentStateEvents=parentStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == true))
            val timedOutChildStateEvents=childStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == true))
            println(s"Some State Data map > 0 timedOutParentStateEvents ${timedOutParentStateEvents}")
            println(s"Some State Data map > 0 timedOutChildStateEvents ${timedOutChildStateEvents}")
            val incomingEventsList=incomingEvents.toList
            val parentEventsIncoming=incomingEventsList.filter(_.eventInfo== raceTrackInfo) // .filter(_.hasTimedOut(inputMap("ExpiryDuration")))
            val childEventsIncoming=incomingEventsList.filter(_.eventInfo== raceInfo)
            val validParentIncomingEvents=parentEventsIncoming.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false)
            val validChildIncomingEvents=childEventsIncoming.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false)
            val timedOutParentEvents=parentEventsIncoming.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == true)
            val timedOutChildEvents=childEventsIncoming.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == true)
            println(s"Some State Data map > 0 timedOutParentEvents ${timedOutParentEvents}")
            println(s"Some State Data map > 0 timedOutChildEvents ${timedOutChildEvents}")
            val validParentEvents=validParentIncomingEvents ++: validParentStateEvents
            val validChildEvents=validChildIncomingEvents ++ validChildStateEvents
            (validParentEvents.size,validChildEvents.size) match {
              case (0,0)=> // empty state
                state.dataMap.empty
                groupState.update(state)
                stateOutClass()
              case (0,x) if x >0 => // DB lookup
                checkIfRaceTrackExists(validChildEvents.head.getRaceInfoRecord) match {
                  case value if value == validChildEvents.head.getRaceInfoRecord.raceTrackID =>
                    state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                    groupState.update(state)
                    stateOutClass(validChildEvents)
                  case _ =>
                    state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                    groupState.update(state)
                    stateOutClass()
                }
              case (x,0) if x >0 => // save parent in state and release
                state.dataMap.put(s"${value}_parent",stateOutClass(validParentEvents).getLatestRecord)
                groupState.update(state)
                stateOutClass(validParentEvents)
              case (x,y) if x >0 && y >0 => // check parent wit child, if not then DB and then release
                validChildEvents.head.getKey == validParentEvents.head.getKey match {
                  case value if value == true =>
                    state.dataMap.put(s"${value}_parent",stateOutClass(validParentEvents).getLatestRecord)
                    state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                    groupState.update(state)
                    stateOutClass(validParentEvents ++ validChildEvents)
                  case value if value == false =>
                    checkIfRaceTrackExists(validChildEvents.head.getRaceInfoRecord) match {
                      case value if value == validChildEvents.head.getRaceInfoRecord.raceTrackID =>
                        state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                        state.dataMap.put(s"${value}_parent",stateOutClass(validParentEvents).getLatestRecord)
                        groupState.update(state)
                        stateOutClass(validChildEvents ++ validParentEvents)
                      case _ =>
                        state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                        state.dataMap.put(s"${value}_parent",stateOutClass(validParentEvents).getLatestRecord)
                        groupState.update(state)
                        stateOutClass()
                    }
                }
            }

        }
      case None =>
        val incomingEventList=incomingEvents.toList
        val parentIncomingEvents= incomingEventList.filter(_.eventInfo == raceTrackEventSource)
        val childIncomingEvents=incomingEventList.filter(_.eventInfo == raceInfoEventSource)
        val validParentEvents=parentIncomingEvents.filter(_.hasTimedOut(inputMap("ExpiryDuration"))==false)
        val validChildEvents=childIncomingEvents.filter(_.hasTimedOut(inputMap("ExpiryDuration"))==false)
        val timedOutParentEvents=parentIncomingEvents.filter(_.hasTimedOut(inputMap("ExpiryDuration"))==true)
        val timedOutChildEvents=childIncomingEvents.filter(_.hasTimedOut(inputMap("ExpiryDuration"))==true)
        println(s"None State Data map 0 timedOutParentEvents ${timedOutParentEvents}")
        println(s"None State Data map 0 timedOutChildEvents ${timedOutChildEvents}")
        (validParentEvents.size,validChildEvents.size) match {
          case (0,0) =>
            groupState.update(stateClass())
            stateOutClass()
          case (0,x) if x >0 =>
            checkIfRaceTrackExists(validChildEvents.head.getRaceInfoRecord) match {
              case value if value == validChildEvents.head.getRaceInfoRecord.raceTrackID =>
                 val newState=getNewState
                newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                groupState.update(newState)
                stateOutClass(validChildEvents)
              case _ =>
                val newState=getNewState
                newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                groupState.update(newState)
                stateOutClass()
            }
          case (x,0) if x >0 => // release parent and update
            val newState= getNewState
            newState.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
            groupState.update(newState)
            stateOutClass(validParentEvents)
          case (x,y) if x >0 && y >0 => // check keys, else db lookup and update
              validChildEvents.head.getKey == validParentEvents.head.getKey match {
                case value if value == true =>
                  val newState=getNewState
                  newState.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
                  newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                  groupState.update(newState)
                  stateOutClass(validChildEvents++ validParentEvents)
                case value if value == false =>
                  val newState=getNewState
                  newState.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
                  newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                  groupState.update(newState)
                  stateOutClass(validParentEvents)
              }
        }
    }

  def getNewState=stateClass()

def checkIfRaceTrackExists(raceInfoRecord:raceInfo) ={
  Class.forName(inputMap("JDBCDriverName"))
  val connection=getJDBCConnection
  val queryToCheck=s"select ${inputMap("raceTrackIdColumn")} from ${inputMap("JDBCDatabase")}.${inputMap("raceTrackTable")} where ${inputMap("raceTrackIdColumn")}='${raceInfoRecord.raceTrackID}'"
  sendResult(connection,Try{getResultSet(queryToCheck,connection).executeQuery} match {
    case Success(resultSet) if resultSet.next == true  => resultSet.getString(inputMap("raceTrackIdColumn"))
    case Success(resultSet) if resultSet.next == false  => ""
    case Failure(f) =>
      println(s"Failed to read data from DB due to ${f.printStackTrace}")
      ""
  })
}
  def sendResult(connection:java.sql.Connection,resultString:String)= {
    connection.close
    resultString
  }

  def getResultSet(queryToRun:String,connection:java.sql.Connection)=connection.prepareStatement(queryToRun)

  def getJDBCConnection=  java.sql.DriverManager.getConnection(s"${inputMap("JDBCUrl")}${inputMap("JDBCDatabase")}?user=${inputMap("JDBCUser")}&password=${inputMap("JDBCPassword")}",getJDBCProps)

  def getJDBCProps={
    val props=getProps
    props.put("url",s"${inputMap("JDBCUrl")}${inputMap("JDBCDatabase")}?user=${inputMap("JDBCUser")}&password=${inputMap("JDBCPassword")}")
    props.put("user",inputMap("JDBCUser"))
    props.put("password",inputMap("JDBCPassword"))
    props
  }

  def getProps= new java.util.Properties

  def groupByKeyFun(incomingRecord:outerSchema) = incomingRecord.eventInfo match {
    case value if value == raceTrackEventSource =>
      parse(incomingRecord.incomingMessage).extract[raceTrackInfo].raceTrackID
    case value if value == raceInfoEventSource =>
      parse(incomingRecord.incomingMessage).extract[raceInfo].raceTrackID
  }



}
