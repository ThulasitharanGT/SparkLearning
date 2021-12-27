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
// /home/raptor/IdeaProjects/SparkLearning/Input/timeSplit2.txt
// /home/raptor/IdeaProjects/SparkLearning/Input/timeSplit3.txt
//   @transient val jodaFormat=DateTimeFormat.forPattern(interTSFormat)
   val simpleDateFormat=new java.text.SimpleDateFormat(inputTSFormat)
   val simpleDateFormatOP=new java.text.SimpleDateFormat(interTSFormat)
   def getJodaFormat=DateTimeFormat.forPattern(interTSFormat)
   val getTimeStamp:(org.apache.spark.sql.Row)=>java.sql.Timestamp = (row:org.apache.spark.sql.Row) => row.getAs[java.sql.Timestamp]("Timestamp")
   def getJodaTime(row:org.apache.spark.sql.Row)=DateTime.parse(getTimeStamp(row).toString, getJodaFormat )
   def getJodaTime(timestampStr:String)=  DateTime.parse(timestampStr, getJodaFormat)
   def getJodaTime(row:org.apache.spark.sql.Row,jodaObj:DateTimeFormatter)=  DateTime.parse(getTimeStamp(row).toString, jodaObj )
   val getJavaTS:(org.joda.time.DateTime)=> java.sql.Timestamp=(dateTime:org.joda.time.DateTime) =>   new java.sql.Timestamp(simpleDateFormatOP.parse(simpleDateFormatOP.format(dateTime.toDate)).getTime)

  def main (args:Array[String]):Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder.getOrCreate
    import spark.implicits._
    val sampleFile = s"hdfs://localhost:8020/user/raptor/inputTimeSeries.txt"
    //  val sampleFile = s"hdfs://localhost:8020/user/raptor/timeSplit2.txt"
    val sampleDataDF = spark.read.format("com.databricks.spark.csv")
      .option("header", "true").option("inferSchema", "false")
      .option("delimiter", "|").load(sampleFile)
      .withColumn("Timestamp", to_timestamp(col("Timestamp"), inputTSFormat)) // changes format here
    //.withColumn("orderCol",row_number.over(Window.partitionBy("User_id").orderBy(asc("Timestamp"))))
    // .as[inputSessions]

    /*
    spark.read.format("csv").option("inferSchema","false").option("header","true").option("delimiter","|").load("hdfs://localhost:8020/user/raptor/timeSplit3.txt").withColumn("Timestamp", to_timestamp(col("Timestamp"), inputTSFormat)).orderBy(col("User_id"), asc("Timestamp")).groupByKey(_.getAs[String]("User_id")).flatMapGroups((x, y) =>flatMapGroupFunction(x,y)).orderBy(asc("_1"),asc("_2")).show(false)

* */
    sampleDataDF.orderBy(col("User_id"), asc("Timestamp")).groupByKey(_.getAs[String]("User_id")).flatMapGroups((x, y) =>flatMapGroupFunction(x,y)).orderBy(asc("_1"), asc("_2")).show(false)

//    implicitly[org.apache.spark.sql.Encoder[inputSessionsInter]].schema
  }

  def flatMapGroupFunction(x:String,y:Iterator[org.apache.spark.sql.Row])={
    val simpleDateFormatOP=new java.text.SimpleDateFormat(interTSFormat)
    def getJodaFormat=DateTimeFormat.forPattern(interTSFormat)
    @transient val jodaFomat=getJodaFormat
    @transient val getTimeStamp:(org.apache.spark.sql.Row)=>java.sql.Timestamp = (row:org.apache.spark.sql.Row) => row.getAs[java.sql.Timestamp]("Timestamp")
     def getJodaTime(row:org.apache.spark.sql.Row)= DateTime.parse(getTimeStamp(row).toString,jodaFomat) // getJodaFormat)
    @transient val getJavaTS:(org.joda.time.DateTime)=> java.sql.Timestamp=(dateTime:org.joda.time.DateTime) =>  new java.sql.Timestamp(simpleDateFormatOP.parse(simpleDateFormatOP.format(dateTime.toDate)).getTime)
    /*{
      println(s"getJavaTS dateTime ${dateTime}")
      println(s"getJavaTS simpleDateFormatOP.format(dateTime.toDate) ${simpleDateFormatOP.format(dateTime.toDate)}")
      println(s"getJavaTS simpleDateFormatOP.parse(dateTime.formatted(interTSFormat)).getTime ${simpleDateFormatOP.parse(simpleDateFormatOP.format(dateTime.toDate)).getTime}")
      println(s"getJavaTS new java.sql.Timestamp(simpleDateFormatOP.parse(dateTime.formatted(interTSFormat)).getTime) ${new java.sql.Timestamp(simpleDateFormatOP.parse(simpleDateFormatOP.format(dateTime.toDate)).getTime)}")
      new java.sql.Timestamp(simpleDateFormatOP.parse(simpleDateFormatOP.format(dateTime.toDate)).getTime)
    }*/
    def timeCompare(startTime:org.joda.time.DateTime,endTime:org.joda.time.DateTime,checkMinutes:Int)=
       startTime.getYear == endTime.year.get match {
         case value if value == true => // same year
           println(s"same year startTime ${startTime} endTime ${endTime}")
           startTime.getDayOfYear == endTime.getDayOfYear match {
             case value if value == true => //same day
               value  match {
                 case value if startTime.plusMinutes(checkMinutes).getMinuteOfDay >= endTime.minuteOfDay.get =>
                   println(s"same day valid startTime ${startTime} endTime ${endTime}")
                   true
                 case value if startTime.plusMinutes(checkMinutes).getMinuteOfDay < endTime.minuteOfDay.get =>
                   println(s"same day broken startTime ${startTime} endTime ${endTime}")
                   false
                }
             case value if value == false =>  //diff day
               println(s"diff day startTime ${startTime} endTime ${endTime}")
               value  match {
                 case value if startTime.plusMinutes(checkMinutes).getDayOfYear != endTime.dayOfYear.get =>
                   println(s"diff day broken 1 startTime ${startTime} endTime ${endTime}")
                   false
                 case value if startTime.plusMinutes(checkMinutes).getDayOfYear == endTime.dayOfYear.get  && (startTime.plusMinutes(checkMinutes).getMinuteOfDay <= checkMinutes ) =>
                   println(s"diff day valid 1 startTime ${startTime} endTime ${endTime}")
                   true
                 case value if endTime.minuteOfDay.get <=checkMinutes && startTime.plusMinutes(checkMinutes).getMinuteOfDay >= endTime.minuteOfDay.get =>
                  println(s"diff day valid startTime ${startTime} endTime ${endTime}")
                   true
                 case value if (endTime.minuteOfDay.get > checkMinutes || endTime.minuteOfDay.get < checkMinutes) && startTime.plusMinutes(checkMinutes).getMinuteOfDay < endTime.minuteOfDay.get =>
                   println(s"diff day broken startTime ${startTime} endTime ${endTime}")
                   false
               }
           }
         case value if value == false => // diff year
           println(s"diff year startTime ${startTime} endTime ${endTime}")
           value  match {
             case value if endTime.dayOfYear.get !=1 =>
               println(s"not jan 1 startTime ${startTime} endTime ${endTime}")
               false
             case value if endTime.getDayOfYear ==1 && endTime.minuteOfDay.get <=checkMinutes &&  startTime.plusMinutes(checkMinutes).getMinuteOfDay >= endTime.minuteOfDay.get =>
               println(s"jan 1 and valid startTime ${startTime} endTime ${endTime}")
               true
             case value if endTime.getDayOfYear ==1 && (endTime.minuteOfDay.get >= checkMinutes || endTime.minuteOfDay.get < checkMinutes) &&  (startTime.plusMinutes(checkMinutes).getMinuteOfDay < endTime.minuteOfDay.get || startTime.plusMinutes(checkMinutes).getMinuteOfDay >= endTime.minuteOfDay.get ) =>
               println(s"jan 1 and broken startTime ${startTime} endTime ${endTime}")
               false
           }
       }
    println(s"userID ${x}")
    @transient  val sessionList = y.toList
    @transient  val eventsPerUserId = sessionList.size - 1
    @transient  var controlVar = true
    println(s"controlVar ${controlVar}")
    @transient  var tmpIndex = 0
    println(s"tmpIndex ${tmpIndex}")
    @transient  var startTime =  getJodaTime(sessionList.head) // DateTime.parse(sessionList.head.getAs[java.sql.Timestamp]("Timestamp").toString, jodaObj )  /////////////////////////////////// pass joda format from here
    println(s"startTime ${startTime}")
    @transient  var endTime = startTime
    println(s"endTime ${endTime}")
    @transient val tmpArrayBuffer = collection.mutable.ArrayBuffer[(String,java.sql.Timestamp,Option[java.sql.Timestamp],String)]() // [inputSessionsInter]()
    while(controlVar)
      tmpIndex match {
        case value if value > eventsPerUserId =>
          println(s"more than index value ${value}")
          controlVar=false
        case value if value < eventsPerUserId =>
          println(s"less than index value ${value}")
          timeCompare(endTime,getJodaTime(sessionList(tmpIndex+1)),45) match {
            case value if value == true =>
             println(s"endTime less than index 45 mins")
              timeCompare(startTime,getJodaTime(sessionList(tmpIndex+1)),120) match {
                case value if value == true =>
                  println(s"startTime less than index 120 mins")
                  endTime=getJodaTime(sessionList(tmpIndex+1))
                case false =>
                 println(s"startTime greater than index 120 mins")
    /*1*/    /*       startTime.getYear == getJodaTime(sessionList(tmpIndex+1)).getYear match {  // will be new starttime
                    case true =>  startTime.getDayOfYear == getJodaTime(sessionList(tmpIndex+1)).getDayOfYear
                      match {
                      case value if value==true =>
                        tmpArrayBuffer+=((x,getJavaTS(startTime),Some(getJavaTS(startTime.plusMinutes(120))) ,"Valid"  ))
                        startTime=getJodaTime(sessionList(tmpIndex+1)).minusMinutes(getJodaTime(sessionList(tmpIndex+1)).getMinuteOfDay - startTime.getMinuteOfDay - 120 )
                        endTime=getJodaTime(sessionList(tmpIndex+1))
                      case value if value==false =>
                        tmpArrayBuffer+=((x,getJavaTS(startTime),Some(getJavaTS(startTime.plusMinutes(120))) ,"Valid" ))
                        startTime=getJodaTime(sessionList(tmpIndex+1)).minusMinutes(getJodaTime(sessionList(tmpIndex+1)).getMinuteOfDay - (120 - ((60*24) - startTime.getMinuteOfDay) ) )
                        endTime=getJodaTime(sessionList(tmpIndex+1))
                    }
                    case false =>
                      startTime.getDayOfYear == (startTime.year.isLeap match {case true => 366 case false => 365}) &&  getJodaTime(sessionList(tmpIndex+1)).getDayOfYear ==1
                      match {
                        case value if value==true =>
                          tmpArrayBuffer+=((x,getJavaTS(startTime),Some(getJavaTS(startTime.plusMinutes(120))) ,"Valid"  ))
                          startTime=getJodaTime(sessionList(tmpIndex+1)).minusMinutes(getJodaTime(sessionList(tmpIndex+1)).getMinuteOfDay - startTime.getMinuteOfDay - 120 ).plusMinutes(1) // plus 1 is to avoid overlap
                          endTime=getJodaTime(sessionList(tmpIndex+1))
                        case value if value==false =>
                          tmpArrayBuffer+=((x,getJavaTS(startTime),Some(getJavaTS(startTime.plusMinutes(120))) ,"Valid" ))
                          startTime=getJodaTime(sessionList(tmpIndex+1)).minusMinutes(getJodaTime(sessionList(tmpIndex+1)).getMinuteOfDay - (120 - ((60*24) - startTime.getMinuteOfDay) ) ).plusMinutes(1) // plus 1 is to avoid overlap
                          endTime=getJodaTime(sessionList(tmpIndex+1))
                      }
                }*/ /*1*/
                  /*2*/
                  tmpArrayBuffer+=((x,getJavaTS(startTime),Some(getJavaTS(startTime.plusMinutes(120))) ,"Valid" ))
                  startTime=startTime.plusMinutes(121)
                  endTime=getJodaTime(sessionList(tmpIndex+1))
                /*2*/
               /*
                //old code
                   tmpArrayBuffer+=((x,getJavaTS(startTime),endTime match {case value if value == startTime => None case value => Some(getJavaTS(value)) },endTime match {case value if value == startTime =>"Broken" case _ =>"Valid"  }))
                  startTime=getJodaTime(sessionList(tmpIndex+1))
                 endTime=startTime
                 */
              }
            case value if value == false =>
              println(s"endTime greater than index 45 mins")
              tmpArrayBuffer+=((x,getJavaTS(startTime),endTime match {case value if value == startTime => None case value => Some(getJavaTS(value)) },endTime match {case value if value == startTime =>"Broken" case _ =>"Valid"  }))
              startTime=getJodaTime(sessionList(tmpIndex+1))
              endTime=startTime
          }
          tmpIndex+=1
        case value if value == eventsPerUserId =>
          println(s"index is equal to last in list")
          startTime match {
            case value if value == endTime =>
              println(s"last record broken")
              tmpArrayBuffer+=((x,getJavaTS(startTime),endTime match {case value if value == startTime => None case value => Some(getJavaTS(value)) },"Broken" ))
            case _ =>
              println(s"last valid")
              tmpArrayBuffer+=((x,getJavaTS(startTime),endTime match {case value if value == startTime => None case value => Some(getJavaTS(value)) },"Valid" ))
          }
          tmpIndex+=1
      }
    tmpArrayBuffer
  }


   /* // adding day logic too
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
          println(s"less than index sessionList(tmpIndex + 1) ${sessionList(tmpIndex + 1)}")
          endTime match { // 45 minutes check, 120 min's check needed
          //  case value if value.year.get == getJodaTime(sessionList(tmpIndex + 1)).getYear =>  // year and day check
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
              println(s"not within 45 minute bound")
              startTime.minuteOfDay.get == value.getMinuteOfDay match {
                case value if value == true =>
                  tmpArrayBuffer += ((x, getJavaTS(endTime), None, "Broken"))
                case value if value == false =>
                  tmpArrayBuffer += ((x, getJavaTS(startTime), Some(getJavaTS(endTime)), "Valid"))
              }
              tmpIndex += 1
              // }
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
    */

  /*
  Timestamp|User_id
  2021-05-01T11:00:00Z|u1
  2021-05-01T13:13:00Z|u1
  2021-05-01T15:00:00Z|u2
  2021-05-01T11:25:00Z|u1
  2021-05-01T15:15:00Z|u2
  2021-05-01T02:13:00Z|u3
  2021-05-03T02:15:00Z|u4
  2021-05-02T11:45:00Z|u1
  2021-05-02T11:00:00Z|u3
  2021-05-03T12:15:00Z|u3
  2021-05-03T11:00:00Z|u4
  2021-05-03T21:00:00Z|u4
  2021-05-04T19:00:00Z|u2
  2021-05-04T09:00:00Z|u3
  2021-05-04T08:15:00Z|u1
  */

  // phase 2


  /*
  2021-05-01T11:00:00Z|u1
  2021-05-01T11:25:00Z|u1
  2021-05-01T13:13:00Z|u1
  2021-05-01T13:23:00Z|u1
  2021-05-01T13:33:00Z|u1
  2021-05-01T13:53:00Z|u1
  2021-05-01T14:10:00Z|u1
  2021-05-01T14:30:00Z|u1
  2021-05-02T11:45:00Z|u1
  2021-05-04T08:15:00Z|u1
  2021-05-01T15:00:00Z|u2
  2021-05-01T15:15:00Z|u2
  2021-05-04T19:00:00Z|u2
  2021-05-01T02:13:00Z|u3
  2021-05-02T11:00:00Z|u3
  2021-05-03T12:15:00Z|u3
  2021-05-04T09:00:00Z|u3
  2021-05-03T02:15:00Z|u4
  2021-05-03T11:00:00Z|u4
  2021-05-03T21:00:00Z|u4
  2021-12-31T02:15:00Z|u4
  2021-12-31T02:20:00Z|u4
  2021-12-31T23:40:00Z|u4
  2021-12-31T23:55:00Z|u4
  2022-01-01T00:10:00Z|u4


  */

}
