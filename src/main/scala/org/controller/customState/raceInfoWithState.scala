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

  val readStreamAndStateOutDF=  spark.readStream.format("kafka")
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
    /*  .writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start
*/

    val raceInfoDF=readStreamAndStateOutDF.flatMap(_.dataList.filter(_.eventInfo== raceInfoEventSource))
      .groupByKey(_.getKey)
      .flatMapGroups((raceTrackID,raceAndRaceTrackEvents) =>
        stateOutClass(raceAndRaceTrackEvents.toList).getLatestRecord
      ) .map(_.getRaceInfoRecord)
      .writeStream.format("console").outputMode("update")
      .option("truncate","false").option("checkpointLocation",inputMap("checkpointRace"))
      .foreach(new raceInfoWriter(inputMap)).start


    val raceTrackInfoDF=readStreamAndStateOutDF.flatMap(_.dataList).filter(_.eventInfo==raceTrackEventSource)
      .groupByKey(_.getKey)
      .mapGroups((raceTrackID,raceTrackRecords)=>
    stateOutClass(raceTrackRecords.toList).getLatestRecord.head.getRaceTrackRecord)
    .writeStream.format("console").outputMode("update")
    .option("truncate","false")
      .option("checkpointLocation",inputMap("checkpointRaceTrack"))
      .foreach(new raceTrackInfoWriter(inputMap))
      .start



// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,net.liftweb:lift-json_2.12:3.5.0,mysql:mysql-connector-java:8.0.27 --class org.controller.customState.raceInfoWithState --num-executors 1 --executor-memory 1g --driver-memory 1g --driver-cores 1 --executor-cores 1 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootStrapServers=localhost:8082,localhost:8083,localhost:8084 topic=tmpTopic ExpiryDuration="10 minutes" JDBCDriverName="com.mysql.jdbc.Driver" raceTrackIdColumn=race_track_id JDBCDatabase=kafka_db raceTrackTable=race_track_info JDBCUrl="jdbc:mysql://localhost:3306/" JDBCUser=raptor JDBCPassword="" checkpointLocation="hdfs://localhost:8020/user/raptor/stream/stateRaceTrack/" startingOffsets=latest

    // embed logic to insert into table
    spark.streams.awaitAnyTermination


  }
  implicit val formats = DefaultFormats


  // use no timeout
  // if you use processing timeout, it runs like RDD
  def stateFunction(key:String,incomingEvents:Iterator[outerSchema],groupState:org.apache.spark.sql.streaming.GroupState[stateClass]):stateOutClass=  groupState.getOption match {
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
            (validParentIncomingEvents.dataList.size,validChildIncomingEvents.dataList.size) match {
               case (0,0)  => // no parent
                 println(s"Some State Data map 0 No parent, No child")
                 state.dataMap= emptyMap
                 groupState.update(state)
                 stateOutClass()
               case (x,0) if x >0 => // parent , no child
                 println(s"Some State Data map 0 parent , No child")
                 state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */ )
                 groupState.update(state)
                 state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                 validParentIncomingEvents //  return parent which came in
               case (0,x) if x >0 =>  // no parent , child
              //  checkIfRaceTrackExists(parse(validChildIncomingEvents.dataList.head.incomingMessage).extract[raceInfo]) match {
                 println(s"Some State Data map 0 No parent , child")
                 checkIfRaceTrackExists(validChildIncomingEvents.dataList.head.getRaceInfoRecord) match {
                  case value if value == validChildIncomingEvents.dataList.head.getKey =>
                    println(s"Some State Data map 0 No parent , child - Release")
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}",List.empty) */ )
                    groupState.update(state)
                    state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                    validChildIncomingEvents  //  release child which comes inside
                  case _ =>
                    println(s"Some State Data map 0 No parent , child - No Release")
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */ )
                    groupState.update(state)
                    state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                    stateOutClass()
                }
              case (x,y) if x >0 && y >0 => // parent and child
                println(s"Some State Data map 0 parent , child")
                validParentIncomingEvents.dataList.head.getKey == validChildIncomingEvents.dataList.head.getKey match {
                  case value if value == true => // get latest and update here
                    println(s"Some State Data map 0 parent , child - release")
                    state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}_child",List.empty) */ )
                    state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}_parent",List.empty) */ )
                    groupState.update(state)
                    state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                    stateOutClass(validChildIncomingEvents.dataList ++ validParentIncomingEvents.dataList) //  release child and parent which comes inside
                  case value if value == false => // just for logic sake
                    println(s"Some State Data map 0 parent , child - No release")
                    checkIfRaceTrackExists(validChildIncomingEvents.dataList.head.getRaceInfoRecord) match {
                      case value if value == validChildIncomingEvents.dataList.head.getKey =>
                        println(s"Some State Data map 0 parent , child - No release check true ")
                        state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.dataList.head.getKey}",List.empty) */ )
                        state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}",List.empty) */ )
                        groupState.update(state)
                        state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                        stateOutClass(validChildIncomingEvents.dataList ++ validParentIncomingEvents.dataList)  //  release child and parent which comes inside
                      case _ =>
                        println(s"Some State Data map 0 parent , child - No release check false ")
                        state.dataMap.put(s"${validChildIncomingEvents.dataList.head.getKey}_child",validChildIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validChildIncomingEvents.head.getKey}",List.empty) */)
                        state.dataMap.put(s"${validParentIncomingEvents.dataList.head.getKey}_parent",validParentIncomingEvents.getLatestRecord /* ++ state.dataMap.getOrElse(s"${validParentIncomingEvents.dataList.head.getKey}",List.empty) */ )
                        groupState.update(state)
                        state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                        stateOutClass() // don't release
                    }
                }
            }
          case value if value > 0 => // data present in stateMap
            val parentStateEvents=state.dataMap.filter(_._1.endsWith("_parent")).map(_._2).toList
            val childStateEvents=state.dataMap.filter(_._1.endsWith("_child")).map(_._2).toList
            val validParentStateEvents=parentStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false))
            val validChildStateEvents=childStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == false))
            val timedOutParentStateEvents=parentStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == true))
            val timedOutChildStateEvents=childStateEvents.flatMap(_.filter(_.hasTimedOut(inputMap("ExpiryDuration")) == true))
            println(s"Some State Data map > 0 timedOutParentStateEvents ${timedOutParentStateEvents}")
            println(s"Some State Data map > 0 timedOutChildStateEvents ${timedOutChildStateEvents}")
            val incomingEventsList=incomingEvents.toList
            val parentEventsIncoming=incomingEventsList.filter(_.eventInfo== raceTrackEventSource) // .filter(_.hasTimedOut(inputMap("ExpiryDuration")))
            val childEventsIncoming=incomingEventsList.filter(_.eventInfo== raceInfoEventSource)
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
                println(s"Some State Data map > 0 no parent no child")
                state.dataMap = emptyMap
                groupState.update(state)
                stateOutClass()
              case (0,x) if x >0 => // DB lookup
                println(s"Some State Data map > 0 no parent, child")
                checkIfRaceTrackExists(validChildEvents.head.getRaceInfoRecord) match {
                  case value if value == validChildEvents.head.getKey =>
                    println(s"Some State Data map > 0 no parent, child - Db lookup match")
                    state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                    groupState.update(state)
                    state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                    stateOutClass(validChildEvents) //  release child
                  case _ =>
                    println(s"Some State Data map > 0 no parent, child - Db lookup no match")
                    state.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                    groupState.update(state)
                    state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                    stateOutClass() //  hold child
                }
              case (x,0) if x >0 => // save parent in state and release
                println(s"Some State Data map > 0 parent, no child ")
                state.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
                groupState.update(state)
                state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                stateOutClass(validParentEvents) //  return child
              case (x,y) if x >0 && y >0 => // check parent wit child, if not then DB and then release
                println(s"Some State Data map > 0 parent, child ")
                validChildEvents.head.getKey == validParentEvents.head.getKey match {
                  case value if value == true =>
                    println(s"Some State Data map > 0 parent, child key match")
                    state.dataMap.put(s"${validParentEvents.head.getRaceTrackRecord.raceTrackID}_parent",stateOutClass(validParentEvents).getLatestRecord)
                    state.dataMap.put(s"${validChildEvents.head.getRaceInfoRecord.raceTrackID}_child",stateOutClass(validChildEvents).getLatestRecord)
                    groupState.update(state)
                    state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                    stateOutClass(validParentEvents ++ validChildEvents)  // release parent from state along with child
                  case value if value == false => // for logics sake
                    println(s"Some State Data map > 0 parent, child key - no match")
                    checkIfRaceTrackExists(validChildEvents.head.getRaceInfoRecord) match {
                      case value if value == validChildEvents.head.getKey =>
                        println(s"Some State Data map > 0 parent, child key - no match - db match ")
                        state.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                        state.dataMap.put(s"${value}_parent",stateOutClass(validParentEvents).getLatestRecord)
                        groupState.update(state)
                        state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                        stateOutClass(validChildEvents ++ validParentEvents)  // release parent from state along with child
                      case _ =>
                        println(s"Some State Data map > 0 parent, child key - no match - db no match ")
                        state.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                        state.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
                        groupState.update(state)
                        state.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
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
          case (0,0) => // no parent, no child
            println(s"None State Data map 0 no parent , no child")
            groupState.update(stateClass())
            stateOutClass()
          case (0,x) if x >0 => // no parent, child
            println(s"None State Data map 0 no parent , child")
            checkIfRaceTrackExists(validChildEvents.head.getRaceInfoRecord) match {
              case value if value == validChildEvents.head.getRaceInfoRecord.raceTrackID =>
                println(s"None State Data map 0 no parent , child - match Db lookup ")
                val newState=getNewState
                newState.dataMap.put(s"${value}_child",stateOutClass(validChildEvents).getLatestRecord)
                groupState.update(newState)
                newState.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                stateOutClass(validChildEvents)  // release child
              case _ =>
                println(s"None State Data map 0 no parent , child - no match Db lookup ")
                val newState=getNewState
                newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                groupState.update(newState)
                newState.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                stateOutClass()  // hold child
            }
          case (x,0) if x >0 => // release parent and update
            println(s"None State Data map 0 parent , no child ")
            val newState= getNewState
            newState.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
            groupState.update(newState)
            newState.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
            stateOutClass(validParentEvents)  // return parent
          case (x,y) if x >0 && y >0 => // check keys, else db lookup and update
            println(s"None State Data map 0 parent , child ")
            validChildEvents.head.getKey == validParentEvents.head.getKey match {
                case value if value == true =>
                  println(s"None State Data map 0 parent , child - key match")
                  val newState=getNewState
                  newState.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
                  newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                  groupState.update(newState)
                  newState.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                  stateOutClass(validChildEvents++ validParentEvents) // return parent and child
                case value if value == false =>
                  println(s"None State Data map 0 parent , child - no key match")
                  val newState=getNewState
                  newState.dataMap.put(s"${validParentEvents.head.getKey}_parent",stateOutClass(validParentEvents).getLatestRecord)
                  newState.dataMap.put(s"${validChildEvents.head.getKey}_child",stateOutClass(validChildEvents).getLatestRecord)
                  groupState.update(newState)
                  newState.dataMap.foreach(x => println(s"dataMap key ${x._1} , value ${x._2}"))
                  stateOutClass(validParentEvents) // return parent  , hold child
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
/*

  def groupByKeyFun(incomingRecord:outerSchema) = incomingRecord.eventInfo match {
    case value if value == raceTrackEventSource =>
      parse(incomingRecord.incomingMessage).extract[raceTrackInfo].raceTrackID
    case value if value == raceInfoEventSource =>
      parse(incomingRecord.incomingMessage).extract[raceInfo].raceTrackID
  }
*/

  def groupByKeyFun(incomingRecord:outerSchema) = incomingRecord.getKey
/*

{"eventInfo":"","incomingMessage":"","incomingTimestamp":""}
// race info layout
{"raceID":"","raceTrackID":"","raceEventDate":"","raceSeason":""}
// race track info layout
{"raceTrackID":"","raceTrackVenue":"","raceTrackName":""}

// parent in table , child sent

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT001\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-09 13:20:46.333"}


// child first (no parent in table) , send child later

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT011\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-09 18:05:46.333"}

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT011\",\"raceTrackVenue\":\"Portugal\",\"raceTrackName\":\"Portimao\"}","incomingTimestamp":"2021-12-09 18:02:46.333"}

// keep parent in state , then send the child .

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT012\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Portimao\"}","incomingTimestamp":"2021-12-09 18:59:46.333"}

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT012\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-09 18:59:46.333"}

 -- send updated ts parent.

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT012\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Portimao\"}","incomingTimestamp":"2021-12-09 19:02:46.333"}



// keep parent in state , then updated parent with ts .

// timed out message

// keep child in state ,send parent after child times out, send latest child parent again


{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT014\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:14:46.333"}

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT014\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:21:46.333"}

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT014\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Portimao\"}","incomingTimestamp":"2021-12-10 10:21:46.333"}



// keep parent in state , send latest record parent again . Send child again.

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT016\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:25:46.333"}

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT016\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:28:46.333"}

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT016\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:25:46.333"}




// Send child , send latest child again (no parent in table), send parent, send parent with updated ts again.

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT017\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:25:46.333"}

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT017\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:28:46.333"}

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT017\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:25:46.333"}

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT017\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:28:46.333"}



// Send child , send expired child again (no parent in table), send parent, send expired parent again.

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT018\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:30:46.333"}
{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT018\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:12:46.333"}
{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT018\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:30:46.333"}
{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT018\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:12:46.333"}


// Send child valid , send expired child (no parent in table), send parent , send expired parent same batch.

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT019\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:40:46.333"}
{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT019\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:12:46.333"}
{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT019\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:40:46.333"}
{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT019\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:12:46.333"}

// Send child , send expired child again (no parent in table), send expired parent , send proper parent again.

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT021\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:32:46.333"}
{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT021\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 20:32:46.333"}
{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT021\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 10:40:46.333"}

{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT021\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Monte-carlo\"}","incomingTimestamp":"2021-12-10 20:32:46.333"}

// same batch parent and child

{"eventInfo":"RaceInfo","incomingMessage":"{\"raceID\":\"RC001\",\"raceTrackID\":\"RT015\",\"raceEventDate\":\"2021-05-01\",\"raceSeason\":\"2021\"}","incomingTimestamp":"2021-12-10 10:24:46.333"}
{"eventInfo":"RaceTrackInfo","incomingMessage":"{\"raceTrackID\":\"RT015\",\"raceTrackVenue\":\"Monaco\",\"raceTrackName\":\"Portimao\"}","incomingTimestamp":"2021-12-10 10:24:46.333"}
*/

}
