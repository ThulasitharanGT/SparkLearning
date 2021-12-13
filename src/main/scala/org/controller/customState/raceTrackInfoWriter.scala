package org.controller.customState

import org.controller.customState.entitiesCustomState.raceTrackInfo
import org.controller.customState.raceInfoStateUtils.getDBDetails

class raceTrackInfoWriter (inputMap:collection.mutable.Map[String,String]) extends raceForEachUtil[raceTrackInfo](getDBDetails(inputMap),inputMap("raceTrackTable"),inputMap("raceTrackIdColumn"))
{

  override def insertRecord(record: raceTrackInfo) = connection.prepareStatement(s"insert into ${inputMap("JDBCDatabase")}.${inputMap("raceTrackTable")} (race_track_id,race_track_name,incoming_timestamp,race_track_venue) values('${record.raceTrackID}','${record.raceTrackName}','${record.incomingTimestamp}','${record.raceTrackVenue}')").executeUpdate

  override def updateRecord(record: raceTrackInfo) = connection.prepareStatement( s"update ${inputMap("JDBCDatabase")}.${inputMap("raceTrackTable")} set race_track_name='${record.raceTrackName}',incoming_timestamp='${record.incomingTimestamp}',race_track_venue='${record.raceTrackVenue}' where race_track_id='${record.raceTrackID}'").executeUpdate

  override def readExistingRecordFromTable(record: raceTrackInfo) =
    connection.prepareStatement(s"""select * from ${inputMap("JDBCDatabase")}.${inputMap("raceTrackTable")} where ${inputMap("raceTrackIdColumn")}='${record.raceTrackID}'""").executeQuery match {
      case value =>
        value.next
        raceTrackInfo(raceTrackID = value.getString("race_track_id"),raceTrackVenue = value.getString("race_track_venue"),raceTrackName = value.getString("race_track_name"),incomingTimestamp =value.getTimestamp("incoming_timestamp") ,dbAction = "update")
    }
}
