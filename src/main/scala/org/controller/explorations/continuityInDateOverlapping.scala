package org.controller.explorations

import org.util.SparkOpener
import org.apache.spark.sql.expressions.Window
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object continuityInDateOverlapping extends SparkOpener {

  case class tmpPerson(id:String,name:String,startDate:String,endDate:String)
  case class tmpPersonCombined(id:String,name:String,startDate:String,endDate:String,combined:String)
  case class tmpPersonCombinedWithDept(id:String,name:String,dept:String,startDate:String,endDate:String) extends Serializable
  case class tmpPersonCombinedFinal(id:String,name:String,dept:String,startDate:String,endDate:String,combined:String)
  case class tmpDateInfo(startDate:String,endDate:String,holidayExclusion:Array[String])
  case class tmpDateInfoFull(startDate:String,endDate:String,holidayExclusion:Array[String],numWeekDaysValid:Int,numWeekends:Int,numWeekDaysInValid:Int)


  def dayDiffWithExcludeWeekendAndHoliday(startDate:String,endDate:String,holidayExclusion:Seq[String]) ={
    @transient val datePattern="yyyy-MM-dd"
    @transient val dateformatter=DateTimeFormat.forPattern(datePattern)
    var numWeekDaysValid=0
    var numWeekends=0
    var numWeekDaysInValid=0
    val holidayExclusionJoda=holidayExclusion.map(dateformatter.parseDateTime(_))
    val startDateJoda=dateformatter.parseDateTime(startDate)
    var startDateJodaLatest=dateformatter.parseDateTime(startDate)
    val endDateJoda=dateformatter.parseDateTime(endDate)
    while (startDateJodaLatest.compareTo(endDateJoda) !=0)
    {
      startDateJodaLatest.getDayOfWeek match {
        case value if value >5 => numWeekends=numWeekends+1
        case value if value <= 5  => holidayExclusionJoda.contains(startDateJodaLatest) match {case value if value == true => numWeekDaysInValid=numWeekDaysInValid+1 case value if value == false => numWeekDaysValid=numWeekDaysValid+1 }
      }
      startDateJodaLatest = startDateJodaLatest.plusDays(1)
    }
    Array(numWeekDaysValid,numWeekends,numWeekDaysInValid)
  }



  def dayDiffWithExcludeWeekendAndHolidayCase(tmpInfo:tmpDateInfo) ={
    @transient val datePattern="yyyy-MM-dd"
    @transient val dateformatter=DateTimeFormat.forPattern(datePattern)
    var numWeekDaysValid=0
    var numWeekends=0
    var numWeekDaysInValid=0
    val holidayExclusionJoda=tmpInfo.holidayExclusion.map(dateformatter.parseDateTime(_))
    val startDateJoda=dateformatter.parseDateTime(tmpInfo.startDate)
    var startDateJodaLatest=dateformatter.parseDateTime(tmpInfo.startDate)
    val endDateJoda=dateformatter.parseDateTime(tmpInfo.endDate)
    while (startDateJodaLatest.compareTo(endDateJoda) !=0)
    {
      startDateJodaLatest.getDayOfWeek match {
        case value if value >5 => numWeekends=numWeekends+1
        case value if value <= 5  => holidayExclusionJoda.contains(startDateJodaLatest) match {case value if value == true => numWeekDaysInValid=numWeekDaysInValid+1 case value if value == false => numWeekDaysValid=numWeekDaysValid+1 }
      }
      startDateJodaLatest = startDateJodaLatest.plusDays(1)
    }
    tmpDateInfoFull(tmpInfo.startDate,tmpInfo.endDate,tmpInfo.holidayExclusion,numWeekDaysValid,numWeekends,numWeekDaysInValid)
  }


  def main(args:Array[String]):Unit={
  val spark=SparkSessionLoc()
  import spark.implicits._
//  val tmpDF=Seq(("001","a","2020-05-01","2020-05-01"),("001","a","2020-05-02","2020-05-02"),("001","a","2020-05-03","2020-05-03"),("001","a","2020-05-05","2020-05-05"),("001","a","2020-05-07","2020-05-07"),("001","a","2020-05-08","2020-05-08")).toDF("id|name|startDate|EndDate".split("\\|").map(_.toString).toSeq :_*).orderBy("id","name","startDate") //"id|name|startDate".split("\\|").toSeq.map(col(_))
    val tmpDF=Seq(("001","a","2020-05-01","2020-05-01"),("001","a","2020-05-02","2020-05-02"),("001","a","2020-05-03","2020-05-03"),("001","a","2020-05-05","2020-05-05"),("001","a","2020-05-07","2020-05-07"),("001","a","2020-05-08","2020-05-08"),("001","a","2020-05-10","2020-05-10")).toDF("id|name|startDate|EndDate".split("\\|").map(_.toString).toSeq :_*) // .orderBy("id","name","startDate") put order by in a smaller filtered data frame to reduce overhead //"id|name|startDate".split("\\|").toSeq.map(col(_))

    val datePattern = "yyyy-MM-dd"
  val dateFormat=DateTimeFormat.forPattern(datePattern)
  // only per record processing will work for these

  // tmpDF.withColumn("leadStartDate",lead(col("startDate"),1).over(Window.orderBy("id","name","startDate")))

  val tmpDFIds=tmpDF.select("id").distinct.collect.map(_(0)).toList
// without overlaps
  val arrayBufferFinal=collection.mutable.ArrayBuffer[tmpPersonCombined]()
  for(tmpDFId <- tmpDFIds)
  {
    val arrayBufferTmp=collection.mutable.ArrayBuffer[tmpPersonCombined]()
    val tempDFOfId=tmpDF.filter(s"id = '${tmpDFId}'").orderBy("id","name","startDate").collect
    val totalRecordsForID=tempDFOfId.size
    var startTime=tempDFOfId(0)(2).toString
    var endTime=tempDFOfId(0)(3).toString
    var shrunkFlag="N"
    for (currentRecordIndex <- 0 to totalRecordsForID -1)
      currentRecordIndex match{
        case value if value == totalRecordsForID -1 =>
          println(s"Inside final record Case for Index - ${currentRecordIndex}")
          val currentRecord=tempDFOfId(currentRecordIndex)
          val prevRecord=tempDFOfId(currentRecordIndex-1)
          val currentStartDate=currentRecord.getString(2)
          val currentEndDate=currentRecord.getString(3)
          val prevStartDate=prevRecord.getString(2)
          val prevEndDate=prevRecord.getString(3)
          val currentEndJoda=dateFormat.parseDateTime(currentEndDate)
          val currentStartJoda=dateFormat.parseDateTime(currentStartDate)
          val prevStartJoda=dateFormat.parseDateTime(prevStartDate)
          val prevEndJoda=dateFormat.parseDateTime(prevEndDate)
        //  println(s"Inside final record Case currentRecord - ${currentRecord}")
        //  println(s"Inside final record Case previousRecord - ${prevRecord}")
        //  println(s"currentStartJoda.getDayOfYear - ${currentStartJoda.getDayOfYear}")
        //  println(s"currentEndJoda.getDayOfYear - ${currentEndJoda.getDayOfYear}")
        //  println(s"prevEndJoda.getDayOfYear - ${prevEndJoda.getDayOfYear}")
        //  println(s"value+1 == currentStartJoda.getDayOfYear - ${prevEndJoda.getDayOfYear +1 == currentStartJoda.getDayOfYear}")
        //  println(s"value < currentEndJoda.getMinuteOfDay - ${prevEndJoda.getDayOfYear  < currentEndJoda.getDayOfYear}")
          prevEndJoda.getDayOfYear match {
            case value if ((value+1 == currentStartJoda.getDayOfYear) && (value < currentEndJoda.getDayOfYear)) =>
              println(s"Inside final record Case continuous case")
              shrunkFlag="Y"
              endTime=currentEndDate
              arrayBufferTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
              case _ =>
                println(s"Inside final record Case not continuous case")
            //    arrayBufferTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)  // previous record if not continuous will be added in previous loop case check
              shrunkFlag="N"
              startTime=currentStartDate
              endTime=currentEndDate
              arrayBufferTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
          }

        case _ =>
          println(s"Inside non final record Case for Index - ${currentRecordIndex}")
          val currentRecord=tempDFOfId(currentRecordIndex)
          val nextRecord=tempDFOfId(currentRecordIndex+1)
          val currentStartDate=currentRecord.getString(2)
          val currentEndDate=currentRecord.getString(3)
          val nextStartDate=nextRecord.getString(2)
          val nextEndDate=nextRecord.getString(3)
          val currentEndJoda=dateFormat.parseDateTime(currentEndDate)
          val currentStartJoda=dateFormat.parseDateTime(currentStartDate)
          val nextStartJoda=dateFormat.parseDateTime(nextStartDate)
          val nextEndJoda=dateFormat.parseDateTime(nextEndDate)
          println(s"Inside non final record Case currentRecord - ${currentRecord}")
          println(s"Inside non final record Case nextRecord - ${nextRecord}")
          currentEndJoda.getDayOfYear match
          {
            case value if ((value+1 == nextStartJoda.getDayOfYear)  && (nextEndJoda.getDayOfYear > value))=>
              println(s"Inside non final record Case continuous")
              endTime=nextEndDate
              shrunkFlag="Y"
            case _ =>
              println(s"Inside non final record Case non continuous")
              arrayBufferTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
              shrunkFlag="N"
              startTime=nextStartDate
              endTime=nextEndDate
          }
      }
    arrayBufferTmp.foreach(x => arrayBufferFinal+= x)
  }
    arrayBufferFinal.toSeq.toDF.show(false)
    tmpDF.show(false)

    // -------------------------------------------------------------------------------------- with overlaps ------------------------------------------------------------------

  val tmpDFOver=Seq(("001","a","aa","2020-05-01","2020-05-03"), ("001","a","aa","2020-05-02","2020-05-04"),
    ("001","a","aa","2020-05-03","2020-05-06"),("001","a","aa","2020-05-08","2020-05-10"),("001","a","aa","2020-05-11","2020-05-17"),
    ("001","a","aa","2020-05-13","2020-05-15"),("001","a","aa","2020-05-19","2020-05-20"),("001","a","aa","2020-05-22","2020-05-24"),
    ("001","a","aa","2020-05-24","2020-05-27"),("001","a","aa","2020-05-25","2020-05-26"),("001","a","aa","2020-05-29","2020-05-30")).toDF("id|name|dept|startDate|EndDate".split("\\|").map(_.toString).toSeq :_*)//.orderBy("id","name","startDate") //"id|name|startDate".split("\\|").toSeq.map(col(_))

  /*  val tmpDFOver=Seq(tmpPersonCombinedWithDept("001","a","aa","2020-05-01","2020-05-03"), tmpPersonCombinedWithDept("001","a","aa","2020-05-02","2020-05-04"),
      tmpPersonCombinedWithDept("001","a","aa","2020-05-03","2020-05-06"),tmpPersonCombinedWithDept("001","a","aa","2020-05-08","2020-05-10")
      ,tmpPersonCombinedWithDept("001","a","aa","2020-05-11","2020-05-17"), tmpPersonCombinedWithDept("001","a","aa","2020-05-13","2020-05-15")
      ,tmpPersonCombinedWithDept("001","a","aa","2020-05-19","2020-05-20"),tmpPersonCombinedWithDept("001","a","aa","2020-05-22","2020-05-24"),
      tmpPersonCombinedWithDept("001","a","aa","2020-05-24","2020-05-27"),tmpPersonCombinedWithDept("001","a","aa","2020-05-25","2020-05-26"),
      tmpPersonCombinedWithDept("001","a","aa","2020-05-29","2020-05-30")).toDF("id|name|dept|startDate|EndDate".split("\\|").map(_.toString).toSeq :_*)//.orderBy("id","name","startDate") //"id|name|startDate".split("\\|").toSeq.map(col(_))
*/

    val tmpDFOverIds=tmpDFOver.select("id").distinct.collect.map(_(0)).toList
    val arrayBufferOverFinal=collection.mutable.ArrayBuffer[tmpPersonCombinedFinal]()

    for(tmpDFIdOver <- tmpDFOverIds) // val tmpDFIdOver=tmpDFOverIds(0)
    {
      val arrayBufferOverTmp=collection.mutable.ArrayBuffer[tmpPersonCombinedFinal]()
      val tempDFOverOfId=tmpDFOver.filter(s"id = '${tmpDFIdOver}'").orderBy("id","name","startDate","EndDate").collect
      val totalRecordsForIDOver=tempDFOverOfId.size
      var startTime=tempDFOverOfId(0)(3).toString
      var endTime=tempDFOverOfId(0)(4).toString
      var shrunkFlag="N"
      for (currentRecordIndex <- 0 to totalRecordsForIDOver -1)
        currentRecordIndex match{
          case value if value == totalRecordsForIDOver -1 =>
            println(s"Inside final record Case for Index - ${currentRecordIndex}")
            val currentRecord=tempDFOverOfId(currentRecordIndex)
            val prevRecord=tempDFOverOfId(currentRecordIndex-1)
            val currentStartDate=currentRecord.getString(3)
            val currentEndDate=currentRecord.getString(4)
            val prevStartDate=prevRecord.getString(3)
            val prevEndDate=prevRecord.getString(4)
            val currentEndJoda=dateFormat.parseDateTime(currentEndDate)
            val currentStartJoda=dateFormat.parseDateTime(currentStartDate)
            val prevStartJoda=dateFormat.parseDateTime(prevStartDate)
            val prevEndJoda=dateFormat.parseDateTime(prevEndDate)
         //  println(s"Inside final record Case currentRecord - ${currentRecord}")
         //  println(s"Inside final record Case previousRecord - ${prevRecord}")
         //  println(s"currentStartJoda.getDayOfYear - ${currentStartJoda.getDayOfYear}")
         //  println(s"currentEndJoda.getDayOfYear - ${currentEndJoda.getDayOfYear}")
         //  println(s"dateFormat.parseDateTime(endTime).getDayOfYear - ${ dateFormat.parseDateTime(endTime).getDayOfYear}")
         //  println(s"value+1 == currentStartJoda.getDayOfYear - ${dateFormat.parseDateTime(endTime).getDayOfYear +1 >= currentStartJoda.getDayOfYear}")
         //  println(s"value <= currentEndJoda.getMinuteOfDay - ${dateFormat.parseDateTime(endTime).getDayOfYear  <= currentEndJoda.getDayOfYear}")
         //  println(s"value >= currentEndJoda.getDayOfYear - ${dateFormat.parseDateTime(endTime).getDayOfYear >= currentEndJoda.getDayOfYear}")
         //  println(s"prevStartJoda.getDayOfYear <= currentStartJoda.getDayOfYear - ${prevStartJoda.getDayOfYear <= currentStartJoda.getDayOfYear}")
            dateFormat.parseDateTime(endTime).getDayOfYear match {
              case value if ((value+1 >= currentStartJoda.getDayOfYear) && (value <= currentEndJoda.getDayOfYear)) =>
                println(s"Inside final record Case continuous -1  case")
                shrunkFlag=shrunkFlag // retain the existing shrunkFlag if it doesn't match, the previous loop would have assigned N and it will continue
                endTime=currentEndDate
                arrayBufferOverTmp+= tmpPersonCombinedFinal(currentRecord.getString(0),currentRecord.getString(1),currentRecord.getString(2),startTime,endTime,shrunkFlag)
              case value if ((value >= currentEndJoda.getDayOfYear) && (prevStartJoda.getDayOfYear <= currentStartJoda.getDayOfYear)) => // overlaps
                println(s"Inside final record Case continuous -2 case")
                shrunkFlag=shrunkFlag // retain the existing shrunkFlag if it doesn't match, the previous loop would have assigned N and it will continue
                endTime=endTime // retain the existing end time if it overlaps
                arrayBufferOverTmp+= tmpPersonCombinedFinal(currentRecord.getString(0),currentRecord.getString(1),currentRecord.getString(2),startTime,endTime,shrunkFlag)
              case _ =>
                println(s"Inside final record Case not continuous case")
                //    arrayBufferTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)  // previous record if not continuous will be added in previous loop case check
                shrunkFlag="N"
                startTime=currentStartDate
                endTime=currentEndDate
                arrayBufferOverTmp+= tmpPersonCombinedFinal(currentRecord.getString(0),currentRecord.getString(1),currentRecord.getString(2),startTime,endTime,shrunkFlag)
            }

          case _ =>
            println(s"Inside non final record Case for Index - ${currentRecordIndex}")
            val currentRecord=tempDFOverOfId(currentRecordIndex)
            val nextRecord=tempDFOverOfId(currentRecordIndex+1)
            val currentStartDate=currentRecord.getString(3)
            val currentEndDate=currentRecord.getString(4)
            val nextStartDate=nextRecord.getString(3)
            val nextEndDate=nextRecord.getString(4)
            val currentEndJoda=dateFormat.parseDateTime(currentEndDate)
            val currentStartJoda=dateFormat.parseDateTime(currentStartDate)
            val nextStartJoda=dateFormat.parseDateTime(nextStartDate)
            val nextEndJoda=dateFormat.parseDateTime(nextEndDate)
            println(s"Inside non final record Case currentRecord - ${currentRecord}")
            println(s"Inside non final record Case nextRecord - ${nextRecord}")
            dateFormat.parseDateTime(endTime).getDayOfYear  match
            {
              case value if ((value+1 >= nextStartJoda.getDayOfYear)  && (nextEndJoda.getDayOfYear >= value))=>
                println(s"Inside non final record Case -1 continuous")
                endTime=nextEndDate
                shrunkFlag="Y"
              case value if ((value+1 >= nextEndJoda.getDayOfYear)  && (currentStartJoda.getDayOfYear <= nextStartJoda.getDayOfYear))=> // overlaps
                println(s"Inside non final record Case -2 continuous")
                endTime=currentEndDate
                shrunkFlag="Y"
              case _ =>
                println(s"Inside non final record Case non continuous")
                arrayBufferOverTmp+= tmpPersonCombinedFinal(currentRecord.getString(0),currentRecord.getString(1),currentRecord.getString(2),startTime,endTime,shrunkFlag)
                shrunkFlag="N"
                startTime=nextStartDate
                endTime=nextEndDate
            }
        }
      arrayBufferOverTmp.foreach(x => arrayBufferOverFinal+= x)
    }
    arrayBufferOverFinal.toSeq.toDF.show(false)
    tmpDFOver.show(false)

    //bonus calculation
    spark.udf.register("dayDiffWithExcludeWeekendAndHoliday",dayDiffWithExcludeWeekendAndHoliday(_:String,_:String,_:Seq[String]))

// usage of date calculator

    /*
    //df way 1
    val tmpCheckDF=Seq(("2020-05-03","2020-06-08",List("2020-05-08","2020-06-05"))).toDF("startDate","endDate","holidayExclusion").select(col("startDate").cast(StringType),col("endDate").cast(StringType),col("holidayExclusion"))
    tmpCheckDF.as[tmpDateInfo].map(dayDiffWithExcludeWeekendAndHolidayCase).show(false)
    //df way 2
    tmpCheckDF.selectExpr("*","dayDiffWithExcludeWeekendAndHoliday(cast(startDate as string),cast(endDate as string),cast(holidayExclusion as array<string>)) as resultDays").selectExpr("startDate","endDate","holidayExclusion","resultDays[0] as numWeekDaysValid","resultDays[1] as numWeekends","resultDays[2] as numWeekDaysInValid").show(false)


    tmpCheckDF.selectExpr("*","dayDiffWithExcludeWeekendAndHoliday(cast(startDate as string),cast(endDate as string),cast(holidayExclusion as array<string>)) as resultDays").selectExpr("startDate","endDate","holidayExclusion","resultDays[0] as numWeekDaysValid","resultDays[1] as numWeekends","resultDays[2] as numWeekDaysInValid").show(false)

    // spark sql way, works with hive table when configured in hive metastore

    tmpCheckDF.createOrReplaceTempView("tmpTable")
    spark.sql("select startDate,endDate,holidayExclusion,dayDiffWithExcludeWeekendAndHoliday(startDate,endDate,holidayExclusion) from tmpTable").show(false)
    */

    val holidayExclusionList=Array("2020-05-03","2020-05-08")
    val finalDF=arrayBufferOverFinal.toSeq.toDF.withColumn("holidayExclusion",lit(holidayExclusionList)).selectExpr("*","dayDiffWithExcludeWeekendAndHoliday(cast(startDate as string),cast(endDate as string),cast(holidayExclusion as array<string>)) as resultDays").selectExpr("id","name","dept","startDate","endDate","holidayExclusion","resultDays[0] as numWeekDaysValid","resultDays[1] as numWeekends","resultDays[2] as numWeekDaysInValid")
   //  val finalDFWithDays=finalDF.select(col("id"), col("name"), col("startDate"), col("endDate"), col("holidayExclusion"), col("numWeekDaysValid"), col("numWeekends"), col("numWeekDaysInValid"))

    val finalDFWithDays=finalDF.groupBy(col("id"), col("name"),col("dept")).agg(min("startDate").as("startDate"), max("endDate").as("endDate"), sum("numWeekDaysValid").as("numWeekDaysValid"), sum("numWeekends").as("numWeekends"), sum("numWeekDaysInValid").as("numWeekDaysInValid")).select(col("id"), col("name"), col("dept"), col("startDate"), col("endDate"),(col("numWeekDaysValid") - col("numWeekDaysInValid") ).as("totalValidDays")   )
    // valid days >5 join it with ref DF and re assign date, the final end date id ref's end date if dept matches. Recalculate valid days and the give bonus for those valid days
    val refDF="""aa|2020-05-04|2020-06-09
bb|2020-05-08|2020-05-12
cc|2020-05-17|2020-05-20
dd|2020-05-11|2020-05-25""".split("\n").map(x => {val xSplitted=x.split("\\|"); (xSplitted(0),xSplitted(1),xSplitted(2))}).toSeq.toDF("dept|startRef|endRef".split("\\|").toSeq:_*)

    val tmpFinalBeforeCalculationBonusDF=finalDFWithDays.filter(col("totalValidDays") > lit(5.toLong)).join(refDF,Seq("dept"),"left").where(refDF("startRef") > finalDFWithDays("startDate") && refDF("startRef") < finalDFWithDays("endDate") && refDF("endRef") > finalDFWithDays("endDate") ).select(finalDFWithDays("id"),finalDFWithDays("name"),finalDFWithDays("dept"),finalDFWithDays("startDate"),refDF("endRef").as("endDate"))

    val tmpFinalCalculationBonusDF= tmpFinalBeforeCalculationBonusDF.withColumn("holidayExclusion",lit(holidayExclusionList)).selectExpr("*","dayDiffWithExcludeWeekendAndHoliday(cast(startDate as string),cast(endDate as string),cast(holidayExclusion as array<string>)) as resultDays").selectExpr("id","name","dept","startDate","endDate","holidayExclusion","resultDays[0] as numWeekDaysValid","resultDays[1] as numWeekends","resultDays[2] as numWeekDaysInValid").select(col("id"), col("name"), col("dept"), col("startDate"), col("endDate"),(col("numWeekDaysValid") - col("numWeekDaysInValid") ).as("totalValidDays"))

    val finalBonusDF= tmpFinalCalculationBonusDF.withColumn("bonusAmount",when(col("totalValidDays")>lit(35),150).when(col("totalValidDays")>29,135).when(col("totalValidDays")>25,125).when(col("totalValidDays")>lit(20),lit(115)).otherwise(lit(100))).select(col("id"), col("name"), col("dept"), col("startDate"), col("endDate"), col("totalValidDays"), col("bonusAmount"),(col("totalValidDays")*col("bonusAmount")).as("eligibleBonus"))
    finalBonusDF.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  //  finalBonusDF.save("") // save it in future path
  }
}
