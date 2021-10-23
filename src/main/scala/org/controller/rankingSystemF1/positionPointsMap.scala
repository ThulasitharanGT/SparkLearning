package org.controller.rankingSystemF1

import org.controller.rankingSystemF1.utils._
import org.util.SparkOpener
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object positionPointsMap extends SparkOpener{

  val spark=SparkSessionLoc()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")
  def getInputFilePath(inputMap:collection.mutable.Map[String,String]) = inputMap("inputPath")

  def getInputDF(inputPath: String)= spark.read.format("csv")
    .option("delimiter","|")
    .option("inferSchema","true")
    .option("header","true")
    .load(s"${inputPath}")
    .withColumn("startDate",col("startDate").cast(DateType))

  def main(args:Array[String]):Unit={
    val inputMap=getInputMap(args)
    val inputPath=getInputFilePath(inputMap)

    val inputDFSortedAndCalculated= dropDupesAndTakeLatestPointsWithRespectToStartDate(getInputDF(inputPath))
    // incoming schema -- position,points,season,startDate

    val insertStatementWithEndDate=s"insert into ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} (position, point, season, start_date, end_date) values (?,?,?,?,?)"
    val insertStatementWithoutEndDate=s"insert into ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} (position, point, season, start_date) values (?,?,?,?)"

    /*
position varchar(10),
point varchar(50),
season integer,
start_date date,
end_date date default
*/
    inputDFSortedAndCalculated.show(false)
    inputDFSortedAndCalculated.as[racePointsInfo].groupByKey(x=>(x.season,x.position)).flatMapGroups((key, value) => {
      val eventList = value.toList.sortBy(x => getDateToLong(x.startDate) < getDateToLong(x.startDate))

      @transient val conn = getJDBCConnection(inputMap)
      @transient var insertQueryWithEndPreparedStatement = conn.prepareStatement(insertStatementWithEndDate)
      @transient var insertQueryWithoutEndPreparedStatement = conn.prepareStatement(insertStatementWithoutEndDate)

      val result=eventList.map { x => {
        getLatestStartDateForNonNullRecordInDB(conn, key._1, key._2, inputMap) match {
          case Some(value) =>
            if (value._2 == "start" && value._1.compareTo(x.startDate) < 0) {
              updateEndDateOfExistingRecord(x, conn, inputMap)
              x.endDate match {
                case Some(endDate) =>
                  insertQueryWithEndPreparedStatement = setPrepareStatementInsertWithEndDate(x, insertQueryWithEndPreparedStatement)
                  insertQueryWithEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,1,0)
                case None =>
                  insertQueryWithoutEndPreparedStatement = setPrepareStatementInsertWithoutEndDate(x, insertQueryWithoutEndPreparedStatement)
                  insertQueryWithoutEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,1,1)
              }
            }
            else if (value._2 == "end" && value._1.compareTo(x.startDate) < 0)
              x.endDate match {
                case Some(endDate) =>
                  insertQueryWithEndPreparedStatement = setPrepareStatementInsertWithEndDate(x, insertQueryWithEndPreparedStatement)
                  insertQueryWithEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,2,0)
                case None =>
                  insertQueryWithoutEndPreparedStatement = setPrepareStatementInsertWithoutEndDate(x, insertQueryWithoutEndPreparedStatement)
                  insertQueryWithoutEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,2,1)
              }

            else if (value._2 == "end" && value._1.compareTo(x.startDate) == 0)
            // deduct one from start date and insert
              x.endDate match {
                case Some(endDate) =>
                  insertQueryWithEndPreparedStatement = setPrepareStatementInsertWithEndDate(x.copy(startDate = new java.sql.Date(datePattern.parse(value._1.toString).getTime + (24L * 69L * 60L * 1000L))), insertQueryWithEndPreparedStatement)
                  insertQueryWithEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,3,0)
                case None =>
                  insertQueryWithoutEndPreparedStatement = setPrepareStatementInsertWithoutEndDate(x.copy(startDate = new java.sql.Date(datePattern.parse(x.startDate.toString).getTime + (24L * 69L * 60L * 1000L))), insertQueryWithoutEndPreparedStatement)
                  insertQueryWithoutEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,3,1)
              }
            else if (value._2 == "end" && value._1.compareTo(x.startDate) > 0 )
            x.endDate match {
              case Some(endDateRecord) =>
                if (value._1.compareTo(endDateRecord) > 0) // no update req
                racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,4,-1)
                else if (value._1.compareTo(endDateRecord) < 0){
                // start date of record is end date of table +1
                  insertQueryWithEndPreparedStatement = setPrepareStatementInsertWithEndDate(x.copy(startDate = new java.sql.Date(datePattern.parse(value._1.toString).getTime + (24L * 69L * 60L * 1000L))), insertQueryWithEndPreparedStatement)
                  insertQueryWithEndPreparedStatement.addBatch
                  racePointsInfoWithMeta(x.position, x.points, x.season, x.startDate, x.endDate, 4, 0)
                }
                else // equal
                  racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,4,-2)
              case None =>
              //   rec startdate is table end+1
                insertQueryWithoutEndPreparedStatement = setPrepareStatementInsertWithoutEndDate(x.copy(startDate = new java.sql.Date(datePattern.parse(value._1.toString).getTime + (24L * 69L * 60L * 1000L))), insertQueryWithoutEndPreparedStatement)
                insertQueryWithoutEndPreparedStatement.addBatch
                racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,4,1)
            }
            else
              racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,5,-1) // no scn no update

          //  check record end date if it exists
          // if no record end date is there just update rec stat dt ts and enddate+1 and new record insert
          // if record end date is eq to db end date then dont update
          // if its there check if record end date is > if greater update end ts and return update 1
          // if its there check if record end date is < if lesser dont update end ts and return update 1

          case None =>
            x.endDate match {
              case Some(endDate) =>
                insertQueryWithEndPreparedStatement = setPrepareStatementInsertWithEndDate(x, insertQueryWithEndPreparedStatement)
                insertQueryWithEndPreparedStatement.addBatch
                racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,5,0)
              case None =>
                insertQueryWithoutEndPreparedStatement = setPrepareStatementInsertWithoutEndDate(x, insertQueryWithoutEndPreparedStatement)
                insertQueryWithoutEndPreparedStatement.addBatch
                racePointsInfoWithMeta(x.position,x.points,x.season,x.startDate,x.endDate,5,1)
            }
        }

      }

      }
      insertQueryWithEndPreparedStatement.executeBatch
      insertQueryWithoutEndPreparedStatement.executeBatch
      conn.close
      result
    }).show(false)


    spark.close


  }

  def updateEndDateOfExistingRecord(record:racePointsInfo,conn:java.sql.Connection,inputMap:collection.mutable.Map[String,String]) =
    conn.prepareStatement(s"update ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} set end_date='${datePattern.format(datePattern.parse(record.startDate.toString).getTime - (24L*60L*60L*1000L))}' where season=${record.season} and position=${record.position} and end_date is null").executeUpdate


  val datePattern=new java.text.SimpleDateFormat("yyyy-MM-dd")
  def getDateToLong(date:java.sql.Date)=datePattern.parse(date.toString).getTime

def setPrepareStatementInsertWithoutEndDate (record:racePointsInfo,prepStatement:java.sql.PreparedStatement) ={
  prepStatement.setInt(1,record.position)
  prepStatement.setInt(2,record.points)
  prepStatement.setInt(3,record.season)
  prepStatement.setDate(4,record.startDate)
  prepStatement
}
  def setPrepareStatementInsertWithEndDate (record:racePointsInfo,prepStatement:java.sql.PreparedStatement) ={
    prepStatement.setInt(1,record.position)
    prepStatement.setInt(2,record.points)
    prepStatement.setInt(3,record.season)
    prepStatement.setDate(4,record.startDate)
    prepStatement.setDate(5,record.endDate.get)
    prepStatement
  }

  def getLatestStartDateForNonNullRecordInDB(conn:java.sql.Connection,season:Int,position:Int,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"select start_date from ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} where season=${season} and position=${position} and end_date is null").executeQuery match{
      case value =>
        value.next match {
          case bool if bool == true => Some(value.getDate(1),"start")
          case bool if bool == false  =>
            conn.prepareStatement(s"select count(1) as recCount from ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} where season=${season} and position=${position}").executeQuery
            match {
              case value =>
                value.next match {
                  case value if value ==true =>
                    conn.prepareStatement(s"select end_date from ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} where season=${season} and position=${position} order by end_date desc limit 1").executeQuery
                    match {
                      case value =>
                        value.next match {
                          case bool if bool== true => Some(value.getDate(1),"end")
                          case bool if bool== false => None
                        }
                    }
                  case value if value ==false =>
                    None
                }
            }
        }
    }


  def dropDupesAndTakeLatestPointsWithRespectToStartDate(df:org.apache.spark.sql.DataFrame)=
    df.withColumn("dupesDropper",row_number.over(Window.partitionBy("season","position","startDate").orderBy(asc("points"))))
      .filter("dupesDropper=1").drop("dupesDropper")
      .withColumn("endDate",lead(col("startDate"),1).over(Window.partitionBy("season","position").orderBy(asc("startDate"))))
      .orderBy(asc("season"), asc("position"), asc("startDate"))
      .withColumn("endDate",when(col("endDate").isNotNull ,date_add(col("endDate"),-1)).otherwise(col("endDate"))) // .withColumnRenamed("position","racePosition")

  def doesRecordExists(conn:java.sql.Connection,incomingRecord:racePointsInfo,inputMap:collection.mutable.Map[String,String])=
    conn.prepareStatement(s"select * from  ${inputMap("schemaName")}.${inputMap("pointsPositionMapTable")} where season=${incomingRecord.season}")
}
