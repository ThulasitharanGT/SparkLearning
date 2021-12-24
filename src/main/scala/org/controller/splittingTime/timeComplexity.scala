package org.controller.splittingTime
import org.apache.spark.sql.functions._

import scala.util.control.Breaks._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
object timeComplexity {
  case class inputSessions(Timestamp:java.sql.Timestamp,User_id:String)
  case class inputSessionsInter(User_id:String,startTimeTs:java.sql.Timestamp,endTimeTs:Option[java.sql.Timestamp],valid:String)

  val inputTSFormat="yyyy-MM-dd'T'HH:mm:ss'Z'"
  val interTSFormat="yyyy-MM-dd HH:mm:ss.S"

//   @transient val jodaFormat=DateTimeFormat.forPattern(interTSFormat)
  @transient val simpleDateFormat=new java.text.SimpleDateFormat(interTSFormat)
  @transient val simpleDateFormatOP=new java.text.SimpleDateFormat(interTSFormat)
  def getJodaFormat=DateTimeFormat.forPattern(interTSFormat)

  @transient def getJodaTime(timestampStr:String)=  DateTime.parse(timestampStr, getJodaFormat)

  @transient val getTimeStamp:(org.apache.spark.sql.Row)=>java.sql.Timestamp = (row:org.apache.spark.sql.Row) => row.getAs[java.sql.Timestamp]("Timestamp")

  @transient def getJodaTime(row:org.apache.spark.sql.Row)=     DateTime.parse(getTimeStamp(row).toString, getJodaFormat )

  @transient def getJodaTime(row:org.apache.spark.sql.Row,jodaObj:DateTimeFormatter)=     DateTime.parse(getTimeStamp(row).toString, jodaObj )

  @transient val getJavaTS:(org.joda.time.DateTime)=> java.sql.Timestamp=(dateTime:org.joda.time.DateTime) => new java.sql.Timestamp(simpleDateFormatOP.parse(dateTime.formatted(interTSFormat)).getTime)

  def main (args:Array[String]):Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder.getOrCreate
    import spark.implicits._
    val sampleFile = s"hdfs://localhost:8020/user/raptor/inputTimeSeries.txt"
    val sampleDataDF = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "false")
      .option("delimiter", "|").load(sampleFile)
      .withColumn("Timestamp", to_timestamp(col("Timestamp"), inputTSFormat)) // changes format here
    //.withColumn("orderCol",row_number.over(Window.partitionBy("User_id").orderBy(asc("Timestamp"))))
    // .as[inputSessions]

    sampleDataDF.orderBy(col("User_id"), asc("Timestamp")).groupByKey(_.getAs[String]("User_id")).flatMapGroups((x, y) =>flatMapGroupFunction(x,y)).show(false)




  }

  def flatMapGroupFunction(x:String,y:Iterator[org.apache.spark.sql.Row])={
    println(s"tmpArrayBuffer x ${x}")
    @transient val tmpArrayBuffer = collection.mutable.ArrayBuffer[(String,java.sql.Timestamp,Option[java.sql.Timestamp],String)]() // [inputSessionsInter]()
    println(s"tmpArrayBuffer ${tmpArrayBuffer}")
    @transient  val sessionList = y.toList
    @transient  val eventsPerUserId = sessionList.size - 1
    @transient  var controlVar = true
    println(s"controlVar ${controlVar}")
    @transient  var tmpIndex = 0
    println(s"tmpIndex ${tmpIndex}")
    val jodaObj=getJodaFormat
    @transient  var startTime = getJodaTime(sessionList.head,jodaObj) /////////////////////////////////// pass joda format from here
    println(s"startTime ${startTime}")
    @transient  var endTime = startTime
    println(s"endTime ${endTime}")
    //  breakable {
    while (controlVar)
      tmpIndex match {
        case value if value > eventsPerUserId =>
          println(s"more than index value ${value}")
          println(s"more than index startTime ${startTime}")
          println(s"more than index endTime ${endTime}")
          controlVar = false
        case value if value < eventsPerUserId =>
          println(s"less than index endTime ${endTime}")
          endTime match { // 45 minutes check, 120 min's check needed
            case value if value.plusMinutes(45).minuteOfDay.get >= getJodaTime(sessionList(tmpIndex + 1)).getMinuteOfDay =>
              println(s"within 45 minute bound")
              getJodaTime(sessionList(tmpIndex + 1)) match {
                case value if startTime.plusMinutes(120).getMinuteOfDay > value.getMinuteOfDay =>
                  println(s"within 120 minute bound")
                  println(s"within 120 minute startTime ${startTime}")
                  println(s"within 120 minute endTime ${endTime}")
                  endTime = getJodaTime(sessionList(tmpIndex + 1))
                case value if startTime.plusMinutes(120).getMinuteOfDay < value.getMinuteOfDay && startTime.getMinuteOfDay != endTime.getMinuteOfDay =>
                  println(s"not within 120 minute bound valid ")
                  println(s"not within 120 minute startTime valid ${startTime}")
                  println(s"not within 120 minute endTime valid ${endTime}")
                  tmpArrayBuffer += ((x, getJavaTS(startTime), Some(getJavaTS(endTime)), "Valid"))
                  startTime = getJodaTime(sessionList(tmpIndex + 1))
                  endTime = startTime
                case value if startTime.plusMinutes(120).getMinuteOfDay < value.getMinuteOfDay && startTime.getMinuteOfDay == endTime.getMinuteOfDay =>
                  println(s"not within 120 minute bound in valid ")
                  println(s"not within 120 minute bound in valid ${startTime} ")
                  println(s"not within 120 minute bound in endTime ${endTime} ")
                  tmpArrayBuffer += ((x, getJavaTS(startTime), None, "Broken"))
                  startTime = getJodaTime(sessionList(tmpIndex + 1))
                  endTime = startTime
              }
              tmpIndex += 1
            case value if value.plusMinutes(45).minuteOfDay.get < getJodaTime(sessionList(tmpIndex + 1)).getMinuteOfDay =>
              startTime.minuteOfDay.get == value.getMinuteOfDay match {
                case value if value == true =>
                  tmpArrayBuffer += ((x, getJavaTS(endTime), None, "Broken"))
                case value if value == false =>
                  tmpArrayBuffer += ((x, getJavaTS(startTime), Some(getJavaTS(endTime)), "Valid"))
              }
              tmpIndex += 1
          }
        case value if value == eventsPerUserId =>
          println(s"equals index endTime ${endTime}")
          startTime.minuteOfDay.get == endTime.getMinuteOfDay match {
            case value if value == true =>
              println(s"equals index broken endTime ${endTime}")
              println(s"equals index broken startTime ${startTime}")
              tmpArrayBuffer+= ((x, getJavaTS(endTime), None, "Broken"))
              controlVar = false
            case value if value == false =>
              println(s"equals index valid endTime ${endTime}")
              println(s"equals index valid startTime ${startTime}")
              tmpArrayBuffer+= ((x, getJavaTS(startTime), Some(getJavaTS(endTime)), "Valid"))
              controlVar = false
          }
      }
    // }
    tmpArrayBuffer
  }
}
