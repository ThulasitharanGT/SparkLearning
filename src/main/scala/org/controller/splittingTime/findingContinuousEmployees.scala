package org.controller.splittingTime

import org.util.{SparkOpener, readWriteUtil}
import org.apache.spark.sql.Row
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable.ListBuffer
import org.controller.AdvancedTopic.empCaseClass
import org.constants.projectConstants

object findingContinuousEmployees extends SparkOpener {
  val dateFormat="yyyy-MM-dd HH:mm:ss"
  def stringToJodaTime(timeString:String) = DateTime.parse(timeString.toString,DateTimeFormat.forPattern(dateFormat))
val spark=SparkSessionLoc("emploees Prog")
  def main(args: Array[String]): Unit = {

  //  val df=spark.read.option("inferSchema","true")/*.option("timestampFormat","yyyy-MM-dd HH:mm:ss")*/.option("header","true").option("delimiter","|").csv("file:///home/raptor/IdeaProjects/SparkLearning/Input/tempEmployeeStartEnd.txt").selectExpr("empId","substring(CAST(startDate as String),0,19) as startDate","substring(CAST(endDate as String),0,19) as endDate")

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
    var finalResultLB:ListBuffer[Row]=new ListBuffer[Row]()
    var finalResultLBExclusion:ListBuffer[Row]=new ListBuffer[Row]()
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
          val nextStartDateExpectedYear=currentEndDate.plusMinutes(60*24).getYear()
          //println("nextStartDateExpectedYear="+nextStartDateExpectedYear)
          val nextStartDateExpectedDay=currentEndDate.plusMinutes(60*24).dayOfYear().getAsText()
          //println("nextStartDateExpectedDay="+nextStartDateExpectedDay)
          val nextStartDateDayYear=nextStartDate.getYear()
          //println("nextStartDateDayYear="+nextStartDateDayYear)
          val nextStartDateDay=nextStartDate.dayOfYear().getAsText()
          //println("nextStartDateDay="+nextStartDateDay)
          nextStartDateDayYear match
          {
            case value if value == nextStartDateExpectedYear  => nextStartDateDay match {case value if value == nextStartDateExpectedDay =>  finalResultLB+=currentEmpArray(i) ; case _ => println("Not continuous date for Emp ID ="+empIdCurrent+" between endTime="+currentEndDate+" and startTime="+nextStartDate);finalResultLBExclusion+=currentEmpArray(i) }
            case _ => println("Not continuous year for Emp ID ="+empIdCurrent+" between endTime="+currentEndDate+" and startTime="+nextStartDate);finalResultLBExclusion+=currentEmpArray(i)
          }
        }
        if(i == currentEmpTotalRecords-1 )
        {
          val currentStartDateString=currentEmpArray(i)(1).toString
          val currentStartDate=stringToJodaTime(currentStartDateString.toString)
          val previousEndDateString=currentEmpArray(i-1)(2).toString
          val previousEndDate=stringToJodaTime(previousEndDateString.toString)
          val prevEndDateExpectedYear=currentStartDate.minusMinutes(60*24).getYear()
          val prevEndDateExpectedDay=currentStartDate.minusMinutes(60*24).dayOfYear().getAsText()
          val prevEndDateYear=previousEndDate.getYear()
          val prevEndDateDay=previousEndDate.dayOfYear().getAsText()
          prevEndDateYear match {
            case value if value == prevEndDateExpectedYear  => prevEndDateExpectedDay match {case value if value == prevEndDateDay =>  finalResultLB+=currentEmpArray(i) ; case _ => println("Not continuous date for Emp ID ="+empIdCurrent+" between prevEndDateDay="+prevEndDateDay+" and prevEndDateExpectedDay="+prevEndDateExpectedDay);finalResultLBExclusion+=currentEmpArray(i)  }
            case _ => println("Not continuous year for Emp ID ="+empIdCurrent+" between prevEndDateDay="+prevEndDateDay+" and prevEndDateExpectedDay="+prevEndDateExpectedDay);finalResultLBExclusion+=currentEmpArray(i)
          }
        }
      }
    }

//case class empCaseClass(empId:String,startDate:String,endDate:String)
    //finalResultLB foreach println
    //finalResultLBExclusion foreach println
    val finalResultDF=finalResultLB.map(_.toSeq).map(r => empCaseClass(r(0).toString,r(1).toString,r(2).toString))//.toDF()  ==> to DF works in repl
    val finalResultLBExclusionDF=finalResultLBExclusion.map(_.toSeq).map(r => empCaseClass(r(0).toString,r(1).toString,r(2).toString))//.toDF()
  }
}
