package org.controller.splittingTime

import org.util.SparkOpener
//import java.text.SimpleDateFormat
//import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.collection.mutable.ListBuffer
//import java.util.Date


object SplittingTimeMain extends SparkOpener  {

val spark=SparkSessionLoc("Testing Minute split")


  def main(args: Array[String]): Unit = {
    val TripRecord=spark.read.format("csv").option("header","true").option("inferSchema","false").option("delimiter","|").load("C:\\Users\\RAPTOR\\IdeaProjects\\SparkLearning\\Input\\InputForSplit.txt")
    //val TripRecord=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","|").load("C:\\Users\\RAPTOR\\IdeaProjects\\SparkLearning\\Input\\InputForSplit.txt")

    //val ReferenceTrip=spark.read.format("csv").option("header","true").option("inferSchema","true").option("delimiter","|").load("C:\\Users\\RAPTOR\\IdeaProjects\\SparkLearning\\Input\\ReferenceForSplit.txt")


    val tripRecordSplitted=TripRecord.map(row => Splitting(row))
    tripRecordSplitted.show
    val tripRecordSplittedExploded=tripRecordSplitted.selectExpr("explode(value) ")
    tripRecordSplittedExploded.map(_.toSeq)

  }


  def Splitting(TripRecord:Row) =
  {
    val dateFormat="YYYY-MM-DD HH:mm:ss"
    var OutputList= new ListBuffer[List[String]]
    val startTimeSource=TripRecord.getString(3)
    val endTimeSource=TripRecord.getString(4)
    val startTime=DateTime.parse(startTimeSource,DateTimeFormat.forPattern(dateFormat))
    val endTime=DateTime.parse(endTimeSource,DateTimeFormat.forPattern(dateFormat))
    var newStartTime=startTime
    var startMinute=0
    var endMinute=15
    val minuteArray=new Array[Int](15)
    var splitNeeded=true
    while(splitNeeded)
    {
      var totalMinute=0
      startMinute=newStartTime.getMinuteOfDay%15
      var newEndTime=newStartTime.plusMinutes(15-newStartTime.getMinuteOfDay%15)
      endMinute=newEndTime.getMinuteOfDay%15
      /*if(newEndTime.compareTo(endTime) > 0)
       {
         splitNeeded=false
         newEndTime=endTime
         endMinute=endTime.getMinuteOfDay%15
       }*/
      newEndTime.compareTo(endTime) match
        {
        case value if value>0 => splitNeeded=false;newEndTime=endTime;endMinute=endTime.getMinuteOfDay%15
        case _ =>   splitNeeded=true
        }
      if (splitNeeded || (endMinute ==0))
        endMinute=15
      for (i <- 0 to 14 )
        i match {
          case value if value >= startMinute && value < endMinute => minuteArray(i)=1;totalMinute+=1
          case _ =>minuteArray(i)=0
        }
      OutputList+=List(TripRecord.getString(0),TripRecord.getString(1),TripRecord.getString(2),newStartTime.toString(),minuteArray(0).toString,minuteArray(1).toString,minuteArray(2).toString,minuteArray(3).toString,minuteArray(4).toString,minuteArray(5).toString,minuteArray(6).toString,minuteArray(7).toString,minuteArray(8).toString,minuteArray(9).toString,minuteArray(10).toString,minuteArray(11).toString,minuteArray(12).toString,minuteArray(13).toString,minuteArray(14).toString,totalMinute.toString)
      newStartTime=newEndTime
    }
    OutputList
  }

/*  val oneMinutesInMilliSeonds=60000
val dateFormat = "YYYY-MM-DD HH:mm:ss"
val formatter = new SimpleDateFormat(dateFormat)

def Splitting(TripRecord:Row) = {  // format in  default one. Joda for any required format
    var OutputList = new ListBuffer[List[String]]
    val startTimeSource =  TripRecord.getString(3)
    val endTimeSource = TripRecord.getString(4)
    println("source start"+startTimeSource)
    println("End start"+endTimeSource)
    val startTime = formatter.parse(startTimeSource)
    val endTime = formatter.parse(endTimeSource)
    println("startTime" + startTime)
    println("EndTime" +endTimeSource)
    var newStartTime = startTime
    var startMinute = 0
    var endMinute = 15
    var minuteArray = new Array[Int](15)
    var splitNeeded = true
    while (splitNeeded) {
      startMinute = milliToMins(newStartTime.getTime) % 15
      val tempMinsToAddToGetEndTime = 15 - milliToMins(newStartTime.getTime) % 15
      var newEndTime = formatter.parse(plusMinutesFun(dateFormat, newStartTime.toString, tempMinsToAddToGetEndTime))
      if (newEndTime.getTime == endTime.getTime)
        splitNeeded = false
      endMinute = milliToMins(newEndTime.getTime) % 15
      if (splitNeeded || (endMinute == 0))
        endMinute = 15
      for (i <- 0 to 14)
        i match {
          case value if value >= startMinute && value < endMinute => minuteArray(i) = 1
          case _ => 0
        }
      OutputList += List(TripRecord.getString(0), TripRecord.getString(1), TripRecord.getString(2), newStartTime.toString(), minuteArray(0).toString, minuteArray(1).toString, minuteArray(2).toString, minuteArray(3).toString, minuteArray(4).toString, minuteArray(5).toString, minuteArray(6).toString, minuteArray(7).toString, minuteArray(8).toString, minuteArray(9).toString, minuteArray(10).toString, minuteArray(11).toString, minuteArray(12).toString, minuteArray(13).toString, minuteArray(14).toString)
      newStartTime = newEndTime
      newEndTime match {
        case endTime => splitNeeded = false
        case _ => splitNeeded = true
      }
    }
    OutputList
 }

    val plusMinutesFun =minutesFun(_:String,_:String,_:Int,"Add")
    val minusMinutesFun =minutesFun(_:String,_:String,_:Int,"Sub")

  def minutesFun(dateFormat:String,dateToModify: String,minsToAppend:Int,functionality:String)=
  {
   val time=formatter.parse(dateToModify).getTime
   var modifiedTime:Long=time
   functionality match {
     case "Add" => modifiedTime=time+(minsToAppend*oneMinutesInMilliSeonds)
     case "Sub" => modifiedTime=time-(minsToAppend*oneMinutesInMilliSeonds)
     case _ =>  println("Wrong Parmeter")
   }
   val finalTime=formatter.format(modifiedTime)
   finalTime.toString
  }

  def milliToMins(timeInMilli:Long)={
    (timeInMilli/oneMinutesInMilliSeonds)toInt
  }

*/
}
