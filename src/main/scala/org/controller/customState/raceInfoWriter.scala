package org.controller.customState

import org.controller.customState.entitiesCustomState.raceInfo
import org.controller.customState.raceInfoStateUtils.getJDBCConnectionObj

import java.sql.Timestamp
import scala.util.{Try,Success,Failure}
class raceInfoWriter(inputMap:collection.mutable.Map[String,String]) extends raceForEachUtil()[raceInfo]{

  @transient var connection: java.sql.Connection = null

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

  override def process(incomingInfo: (raceInfo, Timestamp)): Unit =  doesRecordExist
  insertOrUpdate
  insertRecord
  updateRecord
  deleteRecord

  override def close(errorOrNull: Throwable): Unit = ???





}
