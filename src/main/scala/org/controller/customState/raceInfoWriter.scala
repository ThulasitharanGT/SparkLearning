package org.controller.customState

import org.controller.customState.entitiesCustomState.{dbDetails, raceInfo}
import org.controller.customState.raceInfoStateUtils.getJDBCConnectionObj

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}
import org.controller.customState.raceInfoStateUtils.getDBDetails

class raceInfoWriter(inputMap:collection.mutable.Map[String,String]) extends raceForEachUtil[raceInfo](getDBDetails(inputMap),inputMap("raceInfoTableName"),inputMap("raceIDColumn"))
{
  override def open(partitionId: Long, epochId: Long): Boolean = {
    Try{getJDBCConnectionObj(inputMap)} match {
      case Success(s) =>
        connection=s
        true
      case Failure(f) =>
        println(s"Error opening JDBC connection inside raceInfoWriter ${f.printStackTrace}")
        false
    }
  }

  override def process(incomingInfo: (raceInfo, Timestamp)): Unit = doesRecordExist(incomingInfo._1,connection,inputMap("raceIDColumn"))

  override def readExistingRecordFromTable(record:raceInfo,conn:java.sql.Connection) =
    connection.prepareStatement(s"select * from ${inputMap("JDBCDatabase")}.${inputMap("raceInfoTableName")} where ${inputMap("raceIDColumn")}='${record.raceID}'").executeQuery match {
      case value =>
        value.next
        raceInfo(raceTrackID = )

    }


  override def insertRecord(record:raceInfo,conn:java.sql.Connection)= {

  }
  override def updateRecord(record:raceInfo,conn:java.sql.Connection) ={

  }






}
