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

  val inputMap=collection.mutable.Map[String,String]()

  def main(args:Array[String]):Unit={
    for (arg <- args)
      inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))

    spark.read.format("kafka")
      .option("kafka.bootstrap.servers",inputMap("bootStrapServers"))
      .option("subscribe",inputMap("topic"))
      .option("startingOffsets",inputMap("startingOffsets"))
      .load.select(
      org.apache.spark.sql.functions.from_json(org.apache.spark.sql.functions.col("value").cast(org.apache.spark.sql.types.StringType)
      ,outerStruct).as("message")
    ).select("message.*")
      .select(org.apache.spark.sql.functions.col("eventInfo").cast(org.apache.spark.sql.types.StringType)
      ,org.apache.spark.sql.functions.col("incomingMessage").cast(org.apache.spark.sql.types.StringType)
      ,org.apache.spark.sql.functions.col("incomingTimestamp").cast(org.apache.spark.sql.types.TimestampType))
      .as[outerSchema].groupByKey(groupByKeyFun)
      .mapGroupsWithState( org.apache.spark.sql.streaming.GroupStateTimeout.NoTimeout)(stateFunction)


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
            (validParentIncomingEvents.dataList.size,validChildIncomingEvents.dataList.size)   match {
              case value if value._1 == 0 &&  value._2 ==0 =>
                state.dataMap.empty
                groupState.update(state)
                stateOutClass()
              case value if value._1 == 0  &&  value._2 !=0 =>
                checkIfRaceTrackExists(parse(validChildIncomingEvents.dataList.head.incomingMessage).extract[raceInfo]) match {
                  case value if value == parse(validChildIncomingEvents.dataList.head.incomingMessage).extract[raceInfo].raceTrackID =>
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}",List.empty) */ )
                    groupState.update(state)
                    validChildIncomingEvents
                  case _ =>
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */ )
                    groupState.update(state)
                    stateOutClass()
                }
              case value if value._1 > 0  &&  value._2 !=0 =>
                validParentIncomingEvents.dataList.head.getKey == validChildIncomingEvents.dataList.head.getKey match {
                  case value if value == true => // get latest and update here
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}_child",List.empty) */ )
                    state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}_parent",List.empty) */ )
                    groupState.update(state)
                    stateOutClass(validChildIncomingEvents.dataList ++ validParentIncomingEvents.dataList)
                  case value if value == false => // just for logics sake
                    checkIfRaceTrackExists(parse(validChildIncomingEvents.dataList.head.incomingMessage).extract[raceInfo]) match {
                      case value if value == parse(validChildIncomingEvents.dataList.head.incomingMessage).extract[raceInfo].raceTrackID =>
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
          case value if value >0 =>
            val parentStateEvents=state.dataMap.filter(_._1.endsWith("_parent")).map(_._2).toList.map(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false))
            val childStateEvents=state.dataMap.filter(_._1.endsWith("_child")).map(_._2).toList.map(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false))
            val validParentStateEvents=
            val validChildStateEvents=

        }
      case None =>
    }

def checkIfRaceTrackExists(raceInfoRecord:raceInfo) ={
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

  def getJDBCConnection=  java.sql.DriverManager.getConnection(inputMap("JDBCUrl"),getJDBCProps)

  def getJDBCProps={
    val props=getProps
    props.put("url",inputMap("JDBCUrl"))
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
