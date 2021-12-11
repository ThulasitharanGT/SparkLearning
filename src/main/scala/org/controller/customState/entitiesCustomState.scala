package org.controller.customState

import net.liftweb.json.{DefaultFormats, parse}
import org.controller.customState.customStateConstants._
import raceInfoStateUtils._
object entitiesCustomState {
  implicit val formats = DefaultFormats

  /*
  {"raceID":"","raceTrackID":"","raceEventDate":"","raceSeason":""}

{"raceTrackID":"","raceTrackVenue":"","raceTrackName":""}
 */
  case class outerSchema(eventInfo:String,incomingMessage:String,incomingTimestamp:java.sql.Timestamp) {
    override def toString = s"""{"eventInfo":"${this.eventInfo}","incomingMessage":"${this.incomingMessage}","incomingTimestamp":"${this.incomingTimestamp}"}"""
    def getTs=new java.sql.Timestamp(System.currentTimeMillis)
    def hasTimedOut(durationString:String)= getTs.compareTo(new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(this.incomingTimestamp,durationString))) match {
      case value if List(-1).contains(value) =>
        println(s"hasTimedOut -1 ${getTs}")
        println(s"hasTimedOut -1 ${new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(this.incomingTimestamp,durationString))}")
        false
      case value if List(1,0).contains(value) =>
        println(s"hasTimedOut 1,0 ${getTs}")
        println(s"hasTimedOut 1,0 ${new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(this.incomingTimestamp,durationString))}")
        true
    }
    def getKey=this.eventInfo match {
      case value if value == raceTrackEventSource =>
        parse(this.incomingMessage).extract[raceTrackInfo].raceTrackID
      case value if value == raceInfoEventSource =>
        parse(this.incomingMessage).extract[raceInfo].raceTrackID
    }

    def getRaceTrackRecord= parse(this.incomingMessage).extract[raceTrackInfo].copy(incomingTimestamp=this.incomingTimestamp)
    def getRaceInfoRecord= parse(this.incomingMessage).extract[raceInfo].copy(incomingTimestamp=this.incomingTimestamp)

  }
  type url=String
  type user=String
  type password=String
  type schemaName=String

  case class dbDetails(url:url,user:user,password:password,schemaName:schemaName)
  type tableName=String

  case class stateClass(var dataMap:collection.mutable.Map[String,List[outerSchema]]=collection.mutable.Map[String,List[outerSchema]](""-> List.empty[outerSchema]))
  case class stateOutClass(dataList:List[outerSchema]=List.empty) {
    def getLatestRecord =
      this.dataList.size match {
        case value if value ==0 => List.empty[outerSchema]
        case value if value ==1 => List(dataList.head)
        case value if value >=2 =>
        //   this.dataList.foldLeft[outerSchema](tmpClass:outerSchema)( (a,b) => b )
          List(this.dataList.reduce((a,b) => a.incomingTimestamp.compareTo(b.incomingTimestamp) match {case value if value == -1 => b case value if List(1,0).contains(value) =>a}))
      }
  }
  type dbAction=String
  case class baseForRaceRecords(raceTrackID:String,incomingTimestamp:java.sql.Timestamp,dbAction:dbAction)
  case class raceInfo(raceID:String,raceEventDate:String,raceSeason:String) extends baseForRaceRecords
  case class raceTrackInfo(raceTrackVenue:String,raceTrackName:String) extends baseForRaceRecords
}
