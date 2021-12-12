package org.controller.customState

import org.controller.customState.entitiesCustomState.raceInfo
import org.controller.customState.raceInfoStateUtils.getJDBCConnectionObj

import scala.util.{Failure, Success, Try}
import org.controller.customState.raceInfoStateUtils.getDBDetails

class raceInfoWriter(inputMap:collection.mutable.Map[String,String]) extends raceForEachUtil[raceInfo](getDBDetails(inputMap),inputMap("raceInfoTableName"),inputMap("raceIDColumn"))
{
  override def open(partitionId: Long, epochId: Long): Boolean =
    Try{getJDBCConnectionObj(inputMap)} match {
      case Success(s) =>
        println(s"raceInfoWriter :: Opened connection for partition $partitionId and epoch ${epochId}")
        connection=s
        true
      case Failure(f) =>
        println(s"raceInfoWriter :: Error opening JDBC connection inside raceInfoWriter ${f.printStackTrace} for partition $partitionId and epoch ${epochId}")
        false
    }

 // override def process(incomingInfo: raceInfo): Unit = doesRecordExist(incomingInfo._1,connection,inputMap("raceIDColumn"))

  override def readExistingRecordFromTable(record:raceInfo) =
    connection.prepareStatement(s"select * from ${inputMap("JDBCDatabase")}.${inputMap("raceInfoTableName")} where ${inputMap("raceIDColumn")}='${record.raceID}'").executeQuery match
    {
      case value =>
        value.next
        raceInfo(raceTrackID=value.getString("race_track_id"),raceID=value.getString("race_id"),raceSeason=value.getString("race_season"),incomingTimestamp=value.getTimestamp("incoming_timestamp"),dbAction="update",raceEventDate=value.getString("race_event_date"))
    }

  /**
    create table kafka_db.race_info_state (
    race_id varchar(100),
    race_track_id varchar(100),
    race_season varchar(100),
    incoming_timestamp timestamp default current_timestamp(),
    race_event_date date
   );

   create table kafka_db.race_track_info_state (
    race_track_id varchar(100),
    race_track_name varchar(100),
    incoming_timestamp timestamp default current_timestamp()
   );

   alter table kafka_db.race_track_info_state add column race_track_venue varchar(100);


   */


  override def insertRecord(record:raceInfo)=
    connection.prepareStatement(s""" insert into ${inputMap("JDBCDatabase")}.${inputMap("raceInfoTableName")}(race_id,race_track_id,race_season,incoming_timestamp,race_event_date) values('${record.raceID}','${record.raceTrackID}','${record.raceSeason}','${record.incomingTimestamp}','${record.raceEventDate}') """.stripMargin).executeUpdate

  override def updateRecord(record:raceInfo) =connection.prepareStatement(s""" update ${inputMap("JDBCDatabase")}.${inputMap("raceInfoTableName")} set race_track_id='${record.raceTrackID}',race_season='${record.raceSeason}',incoming_timestamp='${record.incomingTimestamp}',race_event_date='${record.raceEventDate}' where race_id='${record.raceID}' """).executeUpdate







}
