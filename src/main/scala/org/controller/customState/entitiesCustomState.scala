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
      case value if List(1).contains(value) => false
      case value if List(-1,0).contains(value) => true
    }
    def getKey=this.eventInfo match {
      case value if value == raceTrackEventSource =>
        parse(this.incomingMessage).extract[raceTrackInfo].raceTrackID
      case value if value == raceInfoEventSource =>
        parse(this.incomingMessage).extract[raceInfo].raceTrackID
    }
  }
  case class raceInfo(raceID:String,raceTrackID:String,raceEventDate:String,raceSeason:String)
  case class raceTrackInfo(raceTrackID:String,raceTrackVenue:String,raceTrackName:String)
  case class stateClass(dataMap:collection.mutable.Map[String,List[outerSchema]])
  case class stateOutClass(dataList:List[outerSchema]=List.empty) {
    def getLatestRecord =
      this.dataList.size match {
        case value if value ==0 => List.empty[outerSchema]
        case value if value !=0 =>
        //   this.dataList.foldLeft[outerSchema](tmpClass:outerSchema)( (a,b) => b )
          List(this.dataList.reduce((a,b) => a.incomingTimestamp.compareTo(b.incomingTimestamp) match {case value if value ==1 => b case value if List(-1,0).contains(value) =>a}))

      }
  }


}
