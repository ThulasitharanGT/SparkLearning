package org.controller.customState

import org.controller.customState.customStateConstants.jodaTSPattern
import org.controller.customState.entitiesCustomState.outerSchema
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object raceInfoStateUtils {
  val outerStruct=new org.apache.spark.sql.types.StructType(
    Array(org.apache.spark.sql.types.StructField("eventInfo",org.apache.spark.sql.types.StringType,true)
      ,org.apache.spark.sql.types.StructField("incomingMessage",org.apache.spark.sql.types.StringType,true)
      ,org.apache.spark.sql.types.StructField("incomingTimestamp",org.apache.spark.sql.types.StringType,true))
  )

  def getMillis(timeStamp:java.sql.Timestamp,interval:String)=
    interval.split(" ").toSeq.toList match {
      case number :: unitTime =>
        println(s"getMillis number ${number} unitTime ${unitTime}")
        //  val millis= getProperMillis(timeStamp,number.toString.toInt,unitTime.head.toString.trim)
        //  println(s"getMillis :: number ${new java.sql.Timestamp(timeStamp.getTime+millis)}")
        getProperMillis(timeStamp,number.toString.toInt,unitTime.head.toString.trim)
      case Nil =>
        println(s"getMillis Nil ")
        getProperMillis(new java.sql.Timestamp(System.currentTimeMillis),365,"days")
    }
  def getDateTime(timeStamp:java.sql.Timestamp)=DateTime.parse(timeStamp.toString,DateTimeFormat.forPattern(jodaTSPattern))

  val emptyMap = collection.mutable.Map[String,List[outerSchema]]()

  def getProperMillis(timeStamp:java.sql.Timestamp,intValue:Int,timeString:String)= timeString.toList match {
    case firstChar :: remainingChars =>
      println(s"getProperMillis Nil firstChar ${firstChar} remainingChars ${remainingChars}")
      firstChar.toString.toLowerCase match {
        case value if value =="d" =>
          println(s"getProperMillis Nil d")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * hoursInADay * intValue.toLong
        case value if value =="m" && remainingChars.head.toString.toLowerCase=="i" =>
          println(s"getProperMillis Nil m i")
          milliSecondsInASecond * secondsInAMinute * intValue.toLong
        case value if value =="m" && remainingChars.head.toString.toLowerCase=="o" =>
          println(s"getProperMillis Nil m o")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * hoursInADay * daysForMonths(getDateTime(timeStamp),intValue)
        case value if value =="h" =>
          println(s"getProperMillis Nil h")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * intValue.toLong
        case value if value =="s" =>
          println(s"getProperMillis Nil s")
          milliSecondsInASecond * intValue.toLong
        case value if value =="y" =>
          println(s"getProperMillis Nil y")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * hoursInADay * numOfDaysInAYear(getDateTime(timeStamp),intValue)
      }
    case Nil =>
      0L
  }

  val milliSecondsInASecond=1000L
  val secondsInAMinute=60L
  val minutesInAHour=60L
  val hoursInADay=24L

  val numOfDaysInAMonth:((String)=>Int) = (dateString:String)=>  DateTime.parse(dateString,DateTimeFormat.forPattern(jodaTSPattern)) match {
    case value if List(1,3,5,7,8,10,12).contains(value.monthOfYear.get) =>
      31
    case value if List(4,6,9,11).contains(value.monthOfYear.get) =>
      30
    case value if 2== value.monthOfYear.get =>
      value.year.isLeap match {
        case leapTrue if leapTrue == true => 29
        case leapTrue if leapTrue == false => 28
      }
  }

  val numOfDaysInAMonthUpd:((DateTime)=>Int) = (dateObj:DateTime)=>  dateObj match {
    case value if List(1,3,5,7,8,10,12).contains(value.monthOfYear.get) =>
      31
    case value if List(4,6,9,11).contains(value.monthOfYear.get) =>
      30
    case value if 2 == value.monthOfYear.get =>
      value.year.isLeap match {
        case leapTrue if leapTrue == true => 29
        case leapTrue if leapTrue == false => 28
      }
  }

  val numOfDaysInAMonthUpdInt:((Int,Boolean)=>Int) = (days:Int,isLeap:Boolean)=>  days match {
    case value if List(1,3,5,7,8,10,12).contains(value) =>
      31
    case value if List(4,6,9,11).contains(value) =>
      30
    case value if 2 == value =>
      isLeap match {
        case leapTrue if leapTrue == true => 29
        case leapTrue if leapTrue == false => 28
      }
  }


  val numOfDaysYear:((String)=>Int) = (dateString:String)=>  DateTime.parse(dateString,DateTimeFormat.forPattern(jodaTSPattern)) match {
    case value if value.year.isLeap == true => 365
    case value if value.year.isLeap == false => 366
  }

  val numOfDaysInAYear:((DateTime,Int)=>Long) = (dateTimeObj:DateTime,numYears:Int)=>  dateTimeObj match {
    case value  =>
      var daysOfYears =0L
      var dateObj=value
      for(i<- 0 to numYears-1)
        daysOfYears+=( dateObj match {
          case value if value.year.isLeap == true =>
            dateObj=value.plusYears(i)
            i match {
              case yearTmp if yearTmp ==0 =>
                val dayCount=     366L - value.getDayOfYear // match {case value if value <= (31L + 28L) => value case  value => value -1}
                println(s"daysCount case leap year 0 dayCount ${dayCount}")
                dayCount
              case yearTmp if yearTmp == numYears-1  =>
                val dayCount= 366L  -(366L - value.getDayOfYear) // - ( 366L - dateObj.getDayOfYear match {case value if value <= (31L + 28L) => value  case  value => value-1} )
                println(s"daysCount case leap numYears-1 dayCount ${dayCount}")
                dayCount
              case _ =>
                val dayCount= 366L
                println(s"daysCount case leap numYears dayCount ${dayCount}")
                dayCount
            }
          case value if value.year.isLeap == false =>
            dateObj=value.plusYears(1)
            i match {
              case yearTmp if yearTmp ==0 =>
                val dayCount=  365L - value.getDayOfYear // - dateObj.getDayOfYear match {case value if dateObj.minusYears(1).year.isLeap == true && value <= (31L + 28L) => value case  value => value +1}
                println(s"daysCount case No leap year 0 dayCount ${dayCount}")
                dayCount
              case yearTmp if yearTmp == numYears-1  =>
                val dayCount=  365L - ( 365L - value.getDayOfYear ) // - ( 366L - dateObj.getDayOfYear match {case value if dateObj.minusYears(1).year.isLeap == true && value <= (31L + 28L) => value case  value => value +1} )
                println(s"daysCount case No leap year numYears-1 dayCount ${dayCount}")
                dayCount
              case _ =>
                val dayCount=  365L
                println(s"daysCount case No leap year numYears dayCount ${dayCount}")
                dayCount
            }
        })
      daysOfYears
  }

  val numOfDaysInAYearDrilled:((DateTime,Int)=>Long) = (dateTimeObj:DateTime,numYears:Int)=>  dateTimeObj match {
    case value  =>
      var daysOfYears =0L
      val monthOfCurrentObj=value.monthOfYear.get
      for(y<- 0 to numYears-1) {
        y match {
          case 0 =>
            var tmpYear=(dateTimeObj.plusYears(y).getYear,0)
            for ( m <-   wrapperForSeqGenOfMonths(monthOfCurrentObj))
              m match {
                case value if value == monthOfCurrentObj =>
                  /*            println(s"numOfDaysInAYearDrilled :: case 0 :: monthOfCurrentObj ${value}")
                              println(s"numOfDaysInAYearDrilled :: case 0 :: daysOfYears ${daysOfYears} ")
                              println(s"numOfDaysInAYearDrilled :: case 0 :: Year ${dateTimeObj.getYear}  ")
                              println(s"numOfDaysInAYearDrilled :: case 0 :: numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap) ${numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap)} ")
                              println(s"numOfDaysInAYearDrilled :: case 0 :: dateTimeObj.dayOfMonth.get ${dateTimeObj.dayOfMonth.get} ")
                          */  tmpYear=(tmpYear._1,tmpYear._2 + numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap) - dateTimeObj.dayOfMonth.get )
                  daysOfYears+=(numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap) - dateTimeObj.dayOfMonth.get )
                case value  =>
                  /*      println(s"numOfDaysInAYearDrilled :: case 0 ${value} ")
                        println(s"numOfDaysInAYearDrilled :: case 0 :: value daysOfYears ${daysOfYears} ")
                        println(s"numOfDaysInAYearDrilled :: case 0 :: value Year ${dateTimeObj.getYear}  ")
                        println(s"numOfDaysInAYearDrilled :: case 0 :: value numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap) ${numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap)} ")
                        println(s"numOfDaysInAYearDrilled :: case 0 :: value dateTimeObj.dayOfMonth.get ${dateTimeObj.dayOfMonth.get} ")
        */              tmpYear=(tmpYear._1,tmpYear._2+ numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap))
                  daysOfYears+= numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap)
              }
            println(s"numOfDaysInAYearDrilled :: case 0 :: tmpYear ${tmpYear} ")
          case tmp if tmp ==  numYears - 1 =>
            var tmpYear=(dateTimeObj.plusYears(y).getYear,0)
            for ( m <-   wrapperForSeqGenOfMonthsLast(monthOfCurrentObj))
              m match {
                case value if value ==monthOfCurrentObj =>
                  /*     println(s"numOfDaysInAYearDrilled :: case tmp :: monthOfCurrentObj ${value}")
                       println(s"numOfDaysInAYearDrilled :: case tmp :: daysOfYears ${daysOfYears} ")
                       println(s"numOfDaysInAYearDrilled :: case tmp :: Year ${dateTimeObj.plusYears(tmp).getYear} ")
                       println(s"numOfDaysInAYearDrilled :: case tmp :: dateTimeObj.dayOfMonth.get ${dateTimeObj.dayOfMonth.get} ")
               */      tmpYear=(tmpYear._1,tmpYear._2+ dateTimeObj.getDayOfMonth)
                  daysOfYears+= dateTimeObj.getDayOfMonth
                case value =>
                  /*       println(s"numOfDaysInAYearDrilled :: case tmp :: value  ${value}")
                         println(s"numOfDaysInAYearDrilled :: case tmp :: value daysOfYears ${daysOfYears} ")
                         println(s"numOfDaysInAYearDrilled :: case tmp :: value Year ${dateTimeObj.plusYears(tmp).getYear} ")
                         println(s"numOfDaysInAYearDrilled :: case tmp :: value numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap) ${numOfDaysInAMonthUpdInt(value,dateTimeObj.plusYears(y+1).year.isLeap)} ")
                         println(s"numOfDaysInAYearDrilled :: case tmp :: value dateTimeObj.dayOfMonth.get ${dateTimeObj.dayOfMonth.get} ")
                   */    tmpYear=(tmpYear._1,tmpYear._2+ numOfDaysInAMonthUpdInt(value,dateTimeObj.plusYears(y+1).year.isLeap))
                  daysOfYears+= numOfDaysInAMonthUpdInt(value,dateTimeObj.plusYears(y+1).year.isLeap)
              }
            println(s"numOfDaysInAYearDrilled :: case tmpYear :: numYear -1 ${tmpYear} ")
          case yearValue =>
            var tmpYear=(dateTimeObj.plusYears(y).getYear,0)
            for ( m <-   getSeqGenForMonths(1))
              m match {
                case value =>
                  /*    println(s"numOfDaysInAYearDrilled :: case _ :: value monthOfCurrentObj ${value}")
                      println(s"numOfDaysInAYearDrilled :: case _ :: value daysOfYears ${daysOfYears} ")
                      println(s"numOfDaysInAYearDrilled :: case _ :: value Year ${dateTimeObj.plusYears(yearValue).getYear} ")
                      println(s"numOfDaysInAYearDrilled :: case _ :: value numOfDaysInAMonthUpdInt(value,dateTimeObj.year.isLeap) ${numOfDaysInAMonthUpdInt(value,dateTimeObj.plusYears(y+1).year.isLeap)} ")
                      println(s"numOfDaysInAYearDrilled :: case _ :: value dateTimeObj.dayOfMonth.get ${dateTimeObj.dayOfMonth.get} ")
            */        tmpYear=(tmpYear._1,tmpYear._2+  numOfDaysInAMonthUpdInt(value,dateTimeObj.plusYears(y+1).year.isLeap))
                  daysOfYears+= numOfDaysInAMonthUpdInt(value,dateTimeObj.plusYears(y+1).year.isLeap)
              }
            println(s"numOfDaysInAYearDrilled :: case _ :: value tmpYear ${tmpYear} ")
        }
      }
      daysOfYears
  }

  def wrapperForSeqGenOfMonths(startNum:Int)=  getSeqGenForMonths(startNum).slice(0,12-(startNum-1))

  def wrapperForSeqGenOfMonthsLast(startNum:Int)=  getSeqGenForMonths(startNum).slice((12 -startNum)+1 ,12) :+startNum


  val getSeqGenForMonths:(Int)=> Seq[Int] = (startNumber:Int)=>startNumber match {
    case value if value ==1 => (1 to 12)
    case value =>
      var tmpSeq:Seq[Int]=Seq.empty
      var monthCounter=startNumber
      for (i <- 1 to 12)
        monthCounter match {
          case value if value == 12 =>
            tmpSeq= tmpSeq :+ monthCounter
            monthCounter=1
          case value =>
            tmpSeq= tmpSeq :+ monthCounter
            monthCounter=monthCounter+1
        }
      tmpSeq
  }

  val daysForMonths:((DateTime,Int)=>Long) = (dateObj:DateTime,numMonths:Int) => dateObj match {
    case value =>
      println(s"daysForMonths value ")
      val currentYear=value.getYear
      val currentMonthsDays=value.getDayOfYear.toLong
      val finalYear= value.plusMonths(numMonths).getYear.toLong
      val finalMonthDays=value.plusMonths(numMonths).getDayOfYear.toLong
      println(s"daysForMonths value currentMonths ${currentYear}")
      println(s"daysForMonths value currentMonthsDays ${currentMonthsDays}")
      println(s"daysForMonths value finalYear ${finalYear}")
      println(s"daysForMonths value finalMonthDays ${finalMonthDays}")
      finalYear match {
        case fY if fY == currentYear =>
          println(s"daysForMonths fY ")
          finalMonthDays - currentMonthsDays
        case fY if fY > currentYear =>
          val daysInCurrentMonth= numOfDaysInAMonthUpd(value).toLong
          println(s"daysForMonths fY daysInCurrentMonth ${daysInCurrentMonth}")
          var daysOfDiff= daysInCurrentMonth.toLong - value.dayOfMonth.get.toLong
          println(s"daysForMonths fY daysOfDiff ${daysOfDiff}")
          for (i <- 1 to numMonths)
            i match {
              case num if num == numMonths =>
                println(s"daysForMonths num numMonths fY i ${i}")
                //      val daysInCurrentMonth=numOfDaysInAMonthUpd(value.plusMonths(i)).toLong
                //      val dayOfMonth=value.plusMonths(i).dayOfMonth
                daysOfDiff+= value.plusMonths(i).dayOfMonth.get.toLong
                println(s"daysForMonths num numMonths fY i daysOfDiff ${daysOfDiff}")
              case _ =>
                println(s"daysForMonths num fY i ${i}")
                daysOfDiff+= numOfDaysInAMonthUpd(value.plusMonths(i)).toLong
                println(s"daysForMonths num fY i daysOfDiff ${daysOfDiff}")
            }
          println(s"daysForMonths fY daysOfDiff ${daysOfDiff}")
          daysOfDiff
      }
  }


}
