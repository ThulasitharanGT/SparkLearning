package org.controller.explorations

import org.util.SparkOpener
import org.apache.spark.sql.expressions.Window
import org.joda.time.format.DateTimeFormat

object continuityInDateOverlapping extends SparkOpener{
  case class tmpPerson(id:String,name:String,startDate:String,endDate:String)
  case class tmpPersonCombined(id:String,name:String,startDate:String,endDate:String,combined:String)

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

    //  with overlaps

    val tmpDFOver=Seq(("001","a","2020-05-01","2020-05-03"),("001","a","2020-05-02","2020-05-04"),("001","a","2020-05-03","2020-05-06"),("001","a","2020-05-08","2020-05-10"),("001","a","2020-05-11","2020-05-17"),("001","a","2020-05-13","2020-05-15"),("001","a","2020-05-19","2020-05-20"),("001","a","2020-05-22","2020-05-24"),("001","a","2020-05-24","2020-05-27"),("001","a","2020-05-25","2020-05-26"),("001","a","2020-05-29","2020-05-30")).toDF("id|name|startDate|EndDate".split("\\|").map(_.toString).toSeq :_*)//.orderBy("id","name","startDate") //"id|name|startDate".split("\\|").toSeq.map(col(_))
    val tmpDFOverIds=tmpDFOver.select("id").distinct.collect.map(_(0)).toList
    val arrayBufferOverFinal=collection.mutable.ArrayBuffer[tmpPersonCombined]()
    for(tmpDFIdOver <- tmpDFOverIds) // val tmpDFIdOver=tmpDFOverIds(0)
    {
      val arrayBufferOverTmp=collection.mutable.ArrayBuffer[tmpPersonCombined]()
      val tempDFOverOfId=tmpDFOver.filter(s"id = '${tmpDFIdOver}'").orderBy("id","name","startDate","EndDate").collect
      val totalRecordsForIDOver=tempDFOverOfId.size
      var startTime=tempDFOverOfId(0)(2).toString
      var endTime=tempDFOverOfId(0)(3).toString
      var shrunkFlag="N"
      for (currentRecordIndex <- 0 to totalRecordsForIDOver -1)
        currentRecordIndex match{
          case value if value == totalRecordsForIDOver -1 =>
            println(s"Inside final record Case for Index - ${currentRecordIndex}")
            val currentRecord=tempDFOverOfId(currentRecordIndex)
            val prevRecord=tempDFOverOfId(currentRecordIndex-1)
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
                arrayBufferOverTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
              case value if ((value >= currentEndJoda.getDayOfYear) && (prevStartJoda.getDayOfYear <= currentStartJoda.getDayOfYear)) => // overlaps
                println(s"Inside final record Case continuous -2 case")
                shrunkFlag=shrunkFlag // retain the existing shrunkFlag if it doesn't match, the previous loop would have assigned N and it will continue
                endTime=endTime // retain the existing end time if it overlaps
                arrayBufferOverTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
              case _ =>
                println(s"Inside final record Case not continuous case")
                //    arrayBufferTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)  // previous record if not continuous will be added in previous loop case check
                shrunkFlag="N"
                startTime=currentStartDate
                endTime=currentEndDate
                arrayBufferOverTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
            }

          case _ =>
            println(s"Inside non final record Case for Index - ${currentRecordIndex}")
            val currentRecord=tempDFOverOfId(currentRecordIndex)
            val nextRecord=tempDFOverOfId(currentRecordIndex+1)
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
                arrayBufferOverTmp+= tmpPersonCombined(currentRecord.getString(0),currentRecord.getString(1),startTime,endTime,shrunkFlag)
                shrunkFlag="N"
                startTime=nextStartDate
                endTime=nextEndDate
            }
        }
      arrayBufferOverTmp.foreach(x => arrayBufferOverFinal+= x)
    }
    arrayBufferOverFinal.toSeq.toDF.show(false)
    tmpDFOver.show(false)


  }
}
