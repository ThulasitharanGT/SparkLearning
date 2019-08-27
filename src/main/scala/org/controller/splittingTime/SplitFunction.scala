package org.controller.splittingTime


import org.apache.spark.sql.Row
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ListBuffer


class SplitFunction {
def Splitting(TripRecord:Row) =
{
  val dateFormat="YYY-MM-DD HH:mm:ss"
  var OutputList= new ListBuffer[List[String]]
  val startTimeSource=TripRecord.getTimestamp(3).toString
  val endTimeSource=TripRecord.getTimestamp(4).toString
  val startTime=DateTime.parse(startTimeSource,DateTimeFormat.forPattern(dateFormat))
  val endTime=DateTime.parse(endTimeSource,DateTimeFormat.forPattern(dateFormat))
  var newStartTime=startTime
  var startMinute=0
  var endMinute=15
  val minuteArray=new Array[Int](15)
  var splitNeeded=true
  while (splitNeeded)
    {
      startMinute=newStartTime.getMinuteOfDay%15
      val newEndTime=newStartTime.plusMinutes(15-newStartTime.getMinuteOfDay%15)
      if(newEndTime.getMinuteOfDay == endTime.getMillisOfDay)
      splitNeeded=false
    endMinute=newEndTime.getMinuteOfDay%15
    if (splitNeeded || (endMinute ==0))
      endMinute=15

      for (i <- 0 to 14 )
         i match {
           case value if value >= startMinute && value < endMinute => minuteArray(i)=1
           case _ =>0
         }

      OutputList+=List(TripRecord.getString(0),TripRecord.getString(1),TripRecord.getString(2),newStartTime.toString(),minuteArray(0).toString,minuteArray(1).toString,minuteArray(2).toString,minuteArray(3).toString,minuteArray(4).toString,minuteArray(5).toString,minuteArray(6).toString,minuteArray(7).toString,minuteArray(8).toString,minuteArray(9).toString,minuteArray(10).toString,minuteArray(11).toString,minuteArray(12).toString,minuteArray(13).toString,minuteArray(14).toString)
     newStartTime=newEndTime
     newEndTime match { case endTime => splitNeeded=false case _ => splitNeeded=true}
    }
  OutputList
    }

}
