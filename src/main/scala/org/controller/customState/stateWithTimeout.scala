package org.controller.customState

import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.util.SparkOpener

object stateWithTimeout extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  def groupByFun(tempClass:tmpStruct)=tempClass.key
  def groupByFun(tempClass:dataClassWithTimeStamp)=tempClass.key

  case class tmpStruct(key:String,value:dataClass)
  case class dataClass(messageReceived:String)
  case class dataClassWithKey(key:String="",messagesReceived:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]("" -> ""))
  case class wrapperCaseClass(dataMap:collection.mutable.Map[String,String])
// custom timeOut

  case class wrapperCaseClassTimestampVersion(dataList:List[dataClassWithTimeStampWithTimeout]=List.empty)

  case class dataClassWithTimeStamp(key:String,value:String,incomingTimestamp:java.sql.Timestamp=new java.sql.Timestamp(System.currentTimeMillis))
  {
    def hasTimedOut(timeStampInterval:String) = new java.sql.Timestamp(System.currentTimeMillis).compareTo(new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(this.incomingTimestamp,timeStampInterval)) )
      match {
        case value if List(1).contains(value) =>
          println(s"hasTimedOut 1 ${new java.sql.Timestamp(System.currentTimeMillis)} ${new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(this.incomingTimestamp,timeStampInterval))}")
          false
        case value if List(-1,0).contains(value) =>
          println(s"hasTimedOut -1,0 ${new java.sql.Timestamp(System.currentTimeMillis)} ${new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(this.incomingTimestamp,timeStampInterval))}")
          true
      }

    def getTs = new java.sql.Timestamp(System.currentTimeMillis)
    def hasTimedOutInState(timeStampInterval:String) = new java.sql.Timestamp(System.currentTimeMillis - getMillis(getTs,timeStampInterval)).compareTo(this.incomingTimestamp)
    match {
      case value if List(1).contains(value) =>
        println(s"hasTimedOutInState 1 ${new java.sql.Timestamp(System.currentTimeMillis)} ${new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(getTs,timeStampInterval))}")
        true
      case value if List(-1,0).contains(value) =>
        println(s"hasTimedOutInState -1,0 ${new java.sql.Timestamp(System.currentTimeMillis)} ${new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(getTs,timeStampInterval))}")
        false
    }
  }
  case class dataClassWithTimeStampWithTimeout(key:String,value:String,incomingTimestamp:java.sql.Timestamp,timeOut:Boolean)


  def getMillis(timeStamp:java.sql.Timestamp,interval:String)=
    interval.split(" ").toSeq.toList match {
      case number :: unitTime =>
        println(s"getMillis number ${number} unitTime ${unitTime}")
        getProperMillis(timeStamp,number.toString.toInt,unitTime.head.toString.trim)
      case Nil =>
        println(s"getMillis Nil ")
        getProperMillis(new java.sql.Timestamp(System.currentTimeMillis),365,"days")
    }
  def getDateTime(timeStamp:java.sql.Timestamp)=DateTime.parse(timeStamp.toString,DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss.SSS"))

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
  }

  val milliSecondsInASecond=1000L
  val secondsInAMinute=60L
  val minutesInAHour=60L
  val hoursInADay=24L

  val numOfDaysInAMonth:((String)=>Int) = (dateString:String)=>  DateTime.parse(dateString,DateTimeFormat.forPattern("yyyy-MM-dd")) match {
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

  val numOfDaysYear:((String)=>Int) = (dateString:String)=>  DateTime.parse(dateString,DateTimeFormat.forPattern("yyyy-MM-dd")) match {
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
            dateObj=value.plusYears(1)
            365L
          case value if value.year.isLeap == false =>
            dateObj=value.plusYears(1)
            366L
        })
          daysOfYears
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



  // apply your logic here , Remove from map if its past a certain time
  // timeout by me

  def mapGroupFunctionWithCustomTimeOut(key:String,incomingEvents:Iterator[dataClassWithTimeStamp],stateObj:GroupState[wrapperCaseClassTimestampVersion]):List[dataClassWithTimeStampWithTimeout] ={
    stateObj.getOption match {
      case  Some(state)=>
        val eventsFromState=state.dataList.map(x => dataClassWithTimeStampWithTimeout(x.key,x.value,x.incomingTimestamp,dataClassWithTimeStamp(x.key,x.value,x.incomingTimestamp).hasTimedOut(inputMap("ExpiryDuration"))) )
        val validEventsFromState=eventsFromState.filter(_.timeOut==true).map(x => {println(s"valid events in state ${x}");x})
        val inValidEventsFromState=eventsFromState.filter(_.timeOut==false) // .map(x => {println(s"in valid events in state ${x}");x})
        println(s"Some inValidEventsFromState ${inValidEventsFromState}")
        val incomingEventsList=incomingEvents.toList.map(x => dataClassWithTimeStampWithTimeout(x.key,x.value,x.incomingTimestamp,dataClassWithTimeStamp(x.key,x.value,x.incomingTimestamp).hasTimedOut(inputMap("ExpiryDuration"))) )
        val validEventsIncoming= incomingEventsList.filter(_.timeOut == false)
        val invalidEventsIncoming= incomingEventsList.filter(_.timeOut == true)
        println(s"Some invalidEventsIncoming ${invalidEventsIncoming}")
        stateObj.update(wrapperCaseClassTimestampVersion(validEventsIncoming++validEventsFromState))
        validEventsIncoming++validEventsFromState
      case None =>
        val incomingEventsList=incomingEvents.toList.map(x => dataClassWithTimeStampWithTimeout(x.key,x.value,x.incomingTimestamp,dataClassWithTimeStamp(x.key,x.value,x.incomingTimestamp).hasTimedOut(inputMap("ExpiryDuration"))) )
        val validEventsIncoming= incomingEventsList.filter(_.timeOut == false)
        val invalidEventsIncoming= incomingEventsList.filter(_.timeOut == true)
        println(s"None invalidEventsIncoming ${invalidEventsIncoming}")
        stateObj.update(wrapperCaseClassTimestampVersion(validEventsIncoming))
        List.empty
    }
  }


  // timeout by spark
  def mapGroupFunctionWithTimeOut(key:String,incomingEvents:Iterator[tmpStruct],stateObj:GroupState[wrapperCaseClass]):dataClassWithKey ={

    def checkState(key:String,incomingEventsInner:Iterator[tmpStruct],stateObj:GroupState[wrapperCaseClass]) ={
      stateObj.getOption match {
        case Some(state) =>
          println(s"Some State ")
          state.dataMap.filter(_._1.startsWith(key)).size >0 match {
            case true =>
              println(s"Data present in state ")
           //   val tmpDataMap=collection.mutable.Map[String,String]()
           //   val eventsList=incomingEventsInner.toList
         //     eventsList.map(x => tmpDataMap.put(s"${key}_${System.currentTimeMillis}",x.value.messageReceived))
              stateObj.update(wrapperCaseClass(state.dataMap ++tmpListToMapAppender(key,incomingEventsInner.toList) ))
              stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
              dataClassWithKey(key,stateObj.get.dataMap)
            case false =>
              println(s"Data not present in state ")
              stateObj.update(wrapperCaseClass(/*state.dataMap ++*/tmpListToMapAppender(key,incomingEventsInner.toList) ))
              stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
              dataClassWithKey()
          }
        case None =>
          println(s"None State ")
          //    val tmpDataMap=collection.mutable.Map[String,String]()
       //   val eventsList=incomingEventsInner.toList
     //     eventsList.map(x => tmpDataMap.put(s"${key}_${System.currentTimeMillis}",x.value.messageReceived))
          stateObj.update(wrapperCaseClass(tmpListToMapAppender(key,incomingEventsInner.toList)))
          stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
          dataClassWithKey()
      }
    }

    stateObj.hasTimedOut match {
      case value if value == true =>
        println(s"State has timed out ")
        stateObj.setTimeoutDuration(inputMap("ExpiryDuration"))
        stateObj.update(wrapperCaseClass(/*stateObj.get.dataMap.filter(! _._1.startsWith(key)) ++*/ tmpListToMapAppender(key,incomingEvents.toList)))
        checkState(key, incomingEvents, stateObj)
      case value if value== false =>
        println(s"State has not timed out ")
        checkState(key, incomingEvents, stateObj)
    }
  }

  def tmpListToMapAppender(key:String,tmpList:List[tmpStruct])={
    val tmpMap=collection.mutable.Map[String,String]()
    tmpList.map(x => tmpMap.put(s"${key}_${System.currentTimeMillis}",x.value.messageReceived))
    tmpMap
  }

  val inputMap=collection.mutable.Map[String,String]()

  def main(args:Array[String]):Unit ={
    import spark.implicits._

    args.map(_.split("=",2)).map(x => inputMap.put(x(0),x(1)))
/*data will be retained in state for 5 mins, after that cleared,
Funda is, message needs to be sent more than once with same key in order to be released.
If timed out, message again needs to be sent with same key twice, if state has a message for that key then everything will be released

  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --class org.controller.customState.stateWithTimeout --num-executors 1 --executor-memory 1g --driver-memory 1g --driver-cores 1 --executor-cores 1 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootStrapServers=localhost:8082,localhost:8083,localhost:8084 topic=tmpTopic checkpointLocation="hdfs://localhost:8020/user/raptor/stream/state1/" ExpiryDuration="5 minutes"
  */
 /*   // spark timeout
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",inputMap("bootStrapServers"))
      .option("subscribe",inputMap("topic")).load.select(
      org.apache.spark.sql.functions.split(
        org.apache.spark.sql.functions.col("value")
          .cast(org.apache.spark.sql.types.StringType)
          .as("valueCasted"),",").as("valueSplitted"))
      .selectExpr("valueSplitted[0] as key","valueSplitted[1] as value" ).map(x => tmpStruct(x.getAs[String]("key"),dataClass(x.getAs[String]("value"))))
      .groupByKey(groupByFun).mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mapGroupFunctionWithTimeOut) // check event timeout with watermark
      // .mapGroupsWithState(GroupStateTimeout.NoTimeout)(mapGroupFunction) // curried function
      .withColumn("cool",org.apache.spark.sql.functions.lit("cool"))
      .writeStream.format("console").outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start
*/

    // custom timeout

    val dateFormatter= new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")

    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",inputMap("bootStrapServers"))
      .option("subscribe",inputMap("topic")).load.select(
      org.apache.spark.sql.functions.split(
        org.apache.spark.sql.functions.col("value")
         .cast(org.apache.spark.sql.types.StringType)
          .as("valueCasted"),",").as("valueSplitted"))
      .selectExpr("valueSplitted[0] as key","valueSplitted[1] as value" ,"valueSplitted[2] as incomingTimestamp")
      .map(x => {println(s"Data => ${x}");dataClassWithTimeStamp(x.getAs[String]("key"),x.getAs[String]("value"),new java.sql.Timestamp(dateFormatter.parse(x.getAs[String]("incomingTimestamp")).getTime))})
      .groupByKey(groupByFun).mapGroupsWithState(GroupStateTimeout.NoTimeout)(mapGroupFunctionWithCustomTimeOut)
      .withColumn("cool",org.apache.spark.sql.functions.lit("cool"))
      .writeStream.format("console").outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start
    spark.streams.awaitAnyTermination
  }
}
