package org.controller.customState
import org.controller.customState.entitiesCustomState.{baseForRaceRecords, dbDetails}
import scala.util.{Try,Success,Failure}
import raceInfoStateUtils.getJDBCConnection
abstract class raceForEachUtil[T<:baseForRaceRecords](dbDetails:dbDetails,tableName:entitiesCustomState.tableName,idColumn:String) extends org.apache.spark.sql.ForeachWriter[T]
{
  @transient var connection:java.sql.Connection=null

  override def close(errorOrNull: Throwable): Unit = Unit
  override def process(value: T): Unit = checkVerbAndExecuteAction(value)
  override def open(partitionId: Long, epochId: Long) = Try{getJDBCConnection(dbDetails)} match {
    case Success(s) =>
      println(s"raceForEachUtil :: open :: success")
      connection=s
      true
    case Failure(f)  =>
      println(s"raceForEachUtil :: open :: failure ${f.printStackTrace}")
      false
  }

  def doesRecordExist(record:T)=
    connection.prepareStatement(s"select 1 from ${dbDetails.schemaName}.${tableName} where ${idColumn}='${record.raceTrackID}'").executeQuery match {case value if value.next == true => true case _ => false}


  def getLatestRecord(oldRecord:T,newRecord:T)= oldRecord.incomingTimestamp.compareTo(newRecord.incomingTimestamp) match {case value if value == -1 =>  newRecord case value if List(0,1).contains(value) => oldRecord }

  def readExistingRecordFromTable(record:T):T
  def insertRecord(record:T):Int
  def updateRecord(record:T):Int

  def insertOrUpdate(record:T)=doesRecordExist(record) match {
    case value if value ==true =>
      updateRecord(getLatestRecord(readExistingRecordFromTable(record),record))
    case value if value ==false =>
      insertRecord(record)
  }



  def deleteRecord(record:T) =
    connection.prepareStatement(s"delete from ${dbDetails.schemaName}.${tableName} where ${idColumn}='${record.raceTrackID}'").executeUpdate

  def checkVerbAndExecuteAction(record:T)=record.dbAction match {
    case value if value.toLowerCase == "insert" || value.toLowerCase == "update" => insertOrUpdate(record)
      connection.close
    case value if value.toLowerCase == "delete" => deleteRecord(record)
      connection.close
  }
}
