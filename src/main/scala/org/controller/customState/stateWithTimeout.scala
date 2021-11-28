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
    def hasTimedOut(timeStampInterval:String) = new java.sql.Timestamp(System.currentTimeMillis).compareTo(new java.sql.Timestamp(this.incomingTimestamp.getTime + getMillis(timeStampInterval)) )
      match {
        case value if List(1).contains(value) => false
        case value if List(-1,0).contains(value) => true
      }
  }
  case class dataClassWithTimeStampWithTimeout(key:String,value:String,incomingTimestamp:java.sql.Timestamp,timeOut:Boolean)


  def getMillis(interval:String)=
    interval.split(" ").toSeq.toList match {
      case number :: unitTime =>
        println(s"getMillis number ${number} unitTime ${unitTime}")
        getProperMillis(number.toString.toInt,unitTime.head.toString.trim)
      case Nil =>
        println(s"getMillis Nil ")
        getProperMillis(365,"days")
    }

  def getProperMillis(intValue:Int,timeString:String)= timeString.toList match {
    case firstChar :: remainingChars =>
      println(s"getProperMillis Nil firstChar ${firstChar} remainingChars ${remainingChars}")
      firstChar.toString.toLowerCase match {
        case value if value =="d" =>
          println(s"getProperMillis Nil d")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * hoursInADay * intValue.toLong
        case value if value =="m" =>
          println(s"getProperMillis Nil m")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * hoursInADay * daysForMonths(intValue)
        case value if value =="h" =>
          println(s"getProperMillis Nil h")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour *intValue
        case value if value =="s" =>
          println(s"getProperMillis Nil s")
          milliSecondsInASecond * intValue
        case value if value =="y" =>
          println(s"getProperMillis Nil y")
          milliSecondsInASecond * secondsInAMinute * minutesInAHour * hoursInADay * numOfDaysInAYear(intValue)
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

  val numOfDaysYear:((String)=>Int) = (dateString:String)=>  DateTime.parse(dateString,DateTimeFormat.forPattern("yyyy-MM-dd")) match {
    case value if value.year.isLeap == true => 365
    case value if value.year.isLeap == false => 366
  }

  val numOfDaysInAYear:((Int)=>Int) = (numYears:Int)=>  DateTime.parse(new java.sql.Date(System.currentTimeMillis).toString,DateTimeFormat.forPattern("yyyy-MM-dd")) match {
    case value  =>
      var daysOfYears =0
      for(i<- 0 to numYears-1)
        daysOfYears+=( value match {
          case value if value.year.isLeap == true =>
            value.plusYears(1)
            365
          case value if value.year.isLeap == false =>
            value.plusYears(1)
            366
        })
          daysOfYears
  }


  val daysForMonths:((Int)=>Int) = (numMonths:Int) => DateTime.parse(new java.sql.Date(System.currentTimeMillis).toString,DateTimeFormat.forPattern("yyyy-MM-dd")) match {
    case value =>
      value.plusMonths(numMonths).getDayOfYear -  value.getDayOfYear
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
      .map(x => dataClassWithTimeStamp(x.getAs[String]("key"),x.getAs[String]("value"),new java.sql.Timestamp(dateFormatter.parse(x.getAs[String]("incomingTimestamp")).getTime)))
      .groupByKey(groupByFun).mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mapGroupFunctionWithCustomTimeOut)
      .withColumn("cool",org.apache.spark.sql.functions.lit("cool"))
      .writeStream.format("console").outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start
    spark.streams.awaitAnyTermination
  }
}
