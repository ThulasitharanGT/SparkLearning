package org.controller.AdvancedTopic

import org.util.{SparkOpener, readWriteUtil}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
//import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import org.controller.AdvancedTopic.empCaseClassWithWeekend
import org.constants.projectConstants


object findingContinuousEmployeesWithGap extends SparkOpener {
  val dateFormat="yyyy-MM-dd HH:mm:ss"
  def stringToJodaTime(timeString:String) = DateTime.parse(timeString.toString,DateTimeFormat.forPattern(dateFormat))
  val spark=SparkSessionLoc("emploees Prog")

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")

  def main(args:Array[String]):Unit={
    val inputMap= collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.basePathArgConstant,System.getProperty("user.dir")+"/Input/")
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"/Input/tempEmployeeStartEnd.txt")
    val df=readWriteUtil.readDF(spark,inputMap).selectExpr("empId","substring(CAST(startDate as String),0,19) as startDate","substring(CAST(endDate as String),0,19) as endDate")
    val dfOrdered=df.orderBy("empId","startDate") //ordered for taking distinct empid
    //val totEmpCount=dfOrdered.select("empId").distinct.count
    val totEmpIdDistinctId=dfOrdered.select("empId").distinct
    val totEmpIdList=totEmpIdDistinctId.select("empId").rdd.map(r => r(0)).collect()
    var finalResultLB:ListBuffer[empCaseClassWithWeekend]=new ListBuffer[empCaseClassWithWeekend]()
    var finalResultLBExclusion:ListBuffer[empCaseClassWithWeekend]=new ListBuffer[empCaseClassWithWeekend]()
    for(empIdCurrent <- totEmpIdList)
    {
      //println(empIdCurrent)
      val currentEmpDf=dfOrdered.filter("empId="+empIdCurrent)
      currentEmpDf.show
      val currentEmpArray=currentEmpDf.rdd.collect() // gives a array of row's
      val currentEmpTotalRecords=currentEmpDf.count.toInt
      //currentEmpArray foreach println
      for (i <- 0 to currentEmpTotalRecords-1)
      {
        if(i != currentEmpTotalRecords-1 )
        {
          val currentEndDateString=currentEmpArray(i)(2).toString
          val currentEndDate=stringToJodaTime(currentEndDateString.toString)
          //println("currentEndDate="+currentEndDate)
          val nextStartDateString=currentEmpArray(i+1)(1).toString
          val nextStartDate=stringToJodaTime(nextStartDateString.toString)
          //println("nextStartDate="+nextStartDate)
          val endDateDayOfWeek=currentEndDate.dayOfWeek.getAsText
          // val startDateDayOfweek=nextStartDate.dayOfWeek.getAsText
          //val tempTestingDay=currentEndDate.plusMinutes(60 * 24 * 3).dayOfWeek().getAsText()
          var weekendContinuation:String="False"
          var nextStartDateExpectedYear:Int=0
          var nextStartDateExpectedDay:String=null
          endDateDayOfWeek match {
            case "Friday" => {
              nextStartDateExpectedYear = currentEndDate.plusMinutes(60 * 24 * 3).getYear
              nextStartDateExpectedDay = currentEndDate.plusMinutes(60 * 24*3).dayOfYear.getAsText
              weekendContinuation="True"
                             }
            case _ => {
                       nextStartDateExpectedYear = currentEndDate.plusMinutes(60 * 24).getYear
                       nextStartDateExpectedDay=currentEndDate.plusMinutes(60*24).dayOfYear.getAsText
                       }
          }
          //println("nextStartDateExpectedYear="+nextStartDateExpectedYear)
                 //println("nextStartDateExpectedDay="+nextStartDateExpectedDay)
          val nextStartDateDayYearActual=nextStartDate.getYear
          //println("nextStartDateDayYearActual="+nextStartDateDayYearActual)
          val nextStartDateDayExpected=nextStartDate.dayOfYear.getAsText

         // println(endDateDayOfweek)
         // println(tempTestingDay)

          //println("nextStartDateDay="+nextStartDateDay)
          nextStartDateDayYearActual match
          {
            case value if value == nextStartDateExpectedYear  => nextStartDateDayExpected match
                                   {
                                   case value if value == nextStartDateExpectedDay =>  finalResultLB+=empCaseClassWithWeekend(currentEmpArray(i)(0).toString,currentEmpArray(i)(1).toString,currentEmpArray(i)(2).toString,weekendContinuation)
                                   case _ => println("Not continuous date for Emp ID ="+empIdCurrent+" between endTime="+currentEndDate+" and startTime="+nextStartDate);finalResultLBExclusion+=empCaseClassWithWeekend(currentEmpArray(i)(0).toString,currentEmpArray(i)(1).toString,currentEmpArray(i)(2).toString,weekendContinuation)
                                   }
            case _ => println("Not continuous year for Emp ID ="+empIdCurrent+" between endTime="+currentEndDate+" and startTime="+nextStartDate);finalResultLBExclusion+=empCaseClassWithWeekend(currentEmpArray(i)(0).toString,currentEmpArray(i)(1).toString,currentEmpArray(i)(2).toString,weekendContinuation)
          }
        }
        if(i == currentEmpTotalRecords-1 )
        {
          val currentStartDateString=currentEmpArray(i)(1).toString
          val currentStartDate=stringToJodaTime(currentStartDateString.toString)
          val previousEndDateString=currentEmpArray(i-1)(2).toString
          val previousEndDate=stringToJodaTime(previousEndDateString.toString)

          val prevEndDateYear=previousEndDate.getYear()
          val prevEndDateDay=previousEndDate.dayOfYear().getAsText()
          val endDateDayOfWeekExpected=previousEndDate.dayOfWeek.getAsText
          var weekendContinuation="False"
          var prevEndDateExpectedYear: Int=0
          var prevEndDateExpectedDay :String=null
          endDateDayOfWeekExpected match
            {
            case "Friday" =>{
                        prevEndDateExpectedYear=currentStartDate.minusMinutes(60*24*3).getYear
                        prevEndDateExpectedDay=currentStartDate.minusMinutes(60*24*3).dayOfYear.getAsText
                            weekendContinuation="True"
                           }
            case _ => {
              prevEndDateExpectedYear=currentStartDate.minusMinutes(60*24).getYear
              prevEndDateExpectedDay=currentStartDate.minusMinutes(60*24).dayOfYear.getAsText
          }

          }

          prevEndDateYear match {
            case value if value == prevEndDateExpectedYear  => prevEndDateExpectedDay match
                             {
                           case value if value == prevEndDateDay =>  finalResultLB+=empCaseClassWithWeekend(currentEmpArray(i)(0).toString,currentEmpArray(i)(1).toString,currentEmpArray(i)(2).toString,weekendContinuation)
                           case _ => println("Not continuous date for Emp ID ="+empIdCurrent+" between prevEndDateDay="+prevEndDateDay+" and prevEndDateExpectedDay="+prevEndDateExpectedDay);finalResultLBExclusion+=empCaseClassWithWeekend(currentEmpArray(i)(0).toString,currentEmpArray(i)(1).toString,currentEmpArray(i)(2).toString,weekendContinuation)
                             }
            case _ => println("Not continuous year for Emp ID ="+empIdCurrent+" between prevEndDateDay="+prevEndDateDay+" and prevEndDateExpectedDay="+prevEndDateExpectedDay);finalResultLBExclusion+=empCaseClassWithWeekend(currentEmpArray(i)(0).toString,currentEmpArray(i)(1).toString,currentEmpArray(i)(2).toString,weekendContinuation)
          }
        }
      }
    }
    val finalResultDF=finalResultLB.toSeq.toDF.orderBy("empId","startDate")
    val finalResultLBExclusionDF=finalResultLBExclusion.toSeq.toDF.orderBy("empId","startDate")

    println(" continuous records")
    finalResultDF.show(false)

    println(" gap records")
    finalResultLBExclusionDF.show(false)
  }

}
