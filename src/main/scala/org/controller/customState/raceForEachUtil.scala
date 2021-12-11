package org.controller.customState
import org.controller.customState.entitiesCustomState.{baseForRaceRecords, dbDetails}

abstract class raceForEachUtil[T<:baseForRaceRecords](dbDetails:dbDetails,tableName:entitiesCustomState.tableName,idColumn:String) extends org.apache.spark.sql.ForeachWriter[T]
{

  @transient val connection:java.sql.Connection=null

  override def close(errorOrNull: Throwable): Unit = Unit
  override def process(value: T): Unit = checkVerbAndExecuteAction(value,connection)
  override def open(partitionId: Long, epochId: Long): Boolean

  def doesRecordExist(record:T,conn:java.sql.Connection)=
    conn.prepareStatement(s"select 1 from ${dbDetails.schemaName}.${tableName} where ${idColumn}='${record.raceTrackID}'").executeQuery match {case value if value.next == true => true case _ => false}



  def getLatestRecord(oldRecord:T,newRecord:T)= oldRecord.incomingTimestamp.compareTo(newRecord.incomingTimestamp) match {case value if value == -1 =>  newRecord case value if List(0,1).contains(value) => oldRecord }

  def readExistingRecordFromTable(record:T,conn:java.sql.Connection):T

  def insertOrUpdate(record:T,conn:java.sql.Connection)=doesRecordExist(record,conn) match {
    case value if value ==true =>
      updateRecord(getLatestRecord(readExistingRecordFromTable(record,conn),record),conn)
    case value if value ==false =>
      insertRecord(record,conn)
  }
  def insertRecord(record:T,conn:java.sql.Connection):Boolean
  def updateRecord(record:T,conn:java.sql.Connection):Boolean

  def deleteRecord(record:T,conn:java.sql.Connection) =
    conn.prepareStatement(s"delete from ${dbDetails.schemaName}.${tableName} where ${idColumn}='${record.raceTrackID}'").executeUpdate

  def checkVerbAndExecuteAction(record:T,conn:java.sql.Connection)=record.dbAction match {
    case value if value.toLowerCase == "insert" || value.toLowerCase == "update" => insertOrUpdate(record,conn)
    case value if value.toLowerCase == "delete" => deleteRecord(record,conn)
  }
}
