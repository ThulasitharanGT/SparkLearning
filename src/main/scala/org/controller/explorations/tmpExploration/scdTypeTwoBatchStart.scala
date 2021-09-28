package org.controller.explorations.tmpExploration

import org.util.SparkOpener

import java.util.concurrent.ThreadLocalRandom
import scala.util.{Failure, Success, Try}

object scdTypeTwoBatchStart extends SparkOpener {
  val spark = SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._

  val inputMap = collection.mutable.Map[String, String]()
  val formulaOneTeams = Seq("Redbull", "Ferrari", "McLaren", "Alpha-Romeo", "Alpha-Tauri", "Alpine", "Williams")
  val formulaOneSensors = Seq("Driver", "Tyre", "Oil-Pressure", "Fuel-Pump", "MGUK", "MGUH", "Exhaust")
  val stringSeq = (('A' to 'Z') ++ ('a' to 'z')) // .map(_.toString)
//  val numberSeq = 1 to Int.MaxValue // reduce size in local

  val numberSeq = 1 to 1000
 // val doubleSeq = 0.1D to (Int.MaxValue / 10) by 0.1D

 // val bigDecimalSeq = scala.math.BigDecimal("1.0") to (Int.MaxValue / 10) by scala.math.BigDecimal("0.1") // reduce size in local
 val bigDecimalSeq = scala.math.BigDecimal("1.0") to (100) by scala.math.BigDecimal("0.1")
  val doubleSeq =bigDecimalSeq.map(_.toDouble)

  val numberSeqSize = numberSeq.size
  val doubleSeqSize = doubleSeq.size
  val bigDecimalSeqSize = bigDecimalSeq.size
  val teamSeqSize = formulaOneTeams.size
  val sensorSeqSize = formulaOneSensors.size
  val stringSeqSize = stringSeq.size
  val minutesInAnHour = 60
  val secondsInAnMinute = 60
  val milliSecondsInAnSecond = 1000
  val hoursInADay = 24
  val milliSecondsInADay = milliSecondsInAnSecond * secondsInAnMinute * minutesInAnHour * hoursInADay

  var controlDateString: String = null

  def getRandomSensor = formulaOneSensors(ThreadLocalRandom.current.nextInt(0, collectionSizeAdjuster(sensorSeqSize)))

  def getId = getRandomTeam

  def getRandomChar = stringSeq(ThreadLocalRandom.current.nextInt(0, randomIndexForStringAdjusted))

  def getRandomCharStringCasted = getRandomChar.toString

  def randomIndexForStringAdjusted = collectionSizeAdjuster(stringSeqSize)

  def randomIndexForNumAdjusted = collectionSizeAdjuster(numberSeqSize)

  def randomIndexForDoubleAdjusted = collectionSizeAdjuster(doubleSeqSize)
  def randomIndexForBigDecimalAdjusted = collectionSizeAdjuster(bigDecimalSeqSize)

  def getRandomTeam = formulaOneTeams(ThreadLocalRandom.current.nextInt(0, getTeamSeqSizeAdjusted))

  def getRandomNumber = numberSeq(ThreadLocalRandom.current.nextInt(0, randomIndexForNumAdjusted))

  def getRandomDouble = doubleSeq(ThreadLocalRandom.current.nextInt(0, randomIndexForDoubleAdjusted))
  def getRandomBigDecimal= bigDecimalSeq(ThreadLocalRandom.current.nextInt(0, randomIndexForBigDecimalAdjusted))


  def getElementFromSeq(Sequence: Seq[String], index: Int) = Sequence(index)

  def getElementFromSeq(Sequence: Seq[Char], index: Int) = Sequence(index)

  def getElementFromSeq(Sequence: Seq[Int], index: Int) = Sequence(index)

  def getElementFromSeq(Sequence: Seq[Long], index: Int) = Sequence(index)

  def getElementFromSeq(Sequence: Seq[Double], index: Int) = Sequence(index)

  def getTeamSeqSizeAdjusted = collectionSizeAdjuster(teamSeqSize)

  def collectionSizeAdjuster(sizeOfCollection: Int) = sizeOfCollection - 1


//  double // def getRow = (getId, getRandomSensor, getRandomString(), getRandomNumber, getRandomDouble)

  // bigDecimal
  def getRow = (getId, getRandomSensor, getRandomString(), getRandomNumber, getRandomBigDecimal)

  def getRandomString(sizeOfString: Int = 5, resultString: String = ""): String = sizeOfString match {
    case value if value == 0 => resultString
    case value if value > 0 => getRandomString(value - 1, s"${resultString}${getRandomCharStringCasted}")
  }

  def singleRowSeq = Seq(getRow)
  def singleRow= getRow
/* double
  def getRandomSeqOfTuple(sizeofSeq: Int = 100, tmpSeq: Seq[(String, String, String, Int, Double)] = null): Seq[(String, String, String, Int, Double)] = sizeofSeq match { // should be 1000 (less because it is running in local)
    case value if value == 0 => tmpSeq match {
      case value if value == null => Seq.empty
      case value if value != null => tmpSeq
    }
    case value if value != 0 => tmpSeq match {
      case value if value == null => getRandomSeqOfTuple(sizeofSeq - 1, singleRowSeq)
      case value if value != null => getRandomSeqOfTuple(sizeofSeq - 1, tmpSeq ++ singleRowSeq)
    }
  }
  */
// big Decimal
  def getRandomSeqOfTuple(sizeofSeq: Int = 100, tmpSeq: Seq[(String, String, String, Int, scala.math.BigDecimal)] = null): Seq[(String, String, String, Int, scala.math.BigDecimal)] = sizeofSeq match { // should be 1000 (less because it is running in local)
    case value if value == 0 => tmpSeq match {
      case value if value == null => Seq.empty
      case value if value != null => tmpSeq
    }
    case value if value != 0 => tmpSeq match {
      case value if value == null => getRandomSeqOfTuple(sizeofSeq - 1, singleRowSeq)
      case value if value != null => getRandomSeqOfTuple(sizeofSeq - 1, tmpSeq :+ singleRow)
    }
  }

  def getRandomDF(rowsInDF: Int = 1000, tmpDF: org.apache.spark.sql.DataFrame = null): org.apache.spark.sql.DataFrame = rowsInDF match { // should be 10000 (less because it is running in local)
    case value if value == 0 => tmpDF match {
      case value if value == null => createDFFromSeq(getRandomSeqOfTuple())
      case value if value != null => value
    }
    case value if value >= 100 => tmpDF match {
      case value if value == null => getRandomDF(rowsInDF - 100, createDFFromSeq(getRandomSeqOfTuple()))
      case value if value != null => getRandomDF(rowsInDF - 100, getUnionDFWrapper(value))
    }
    case value if value < 100 => tmpDF match {
      case value if value == null => getRandomDF(0, createDFFromSeq(getRandomSeqOfTuple(rowsInDF)))
      case value if value != null => getRandomDF(0, getUnionDFWrapper(value, rowsInDF))
    }
  }

  //  def emptyDF=Seq.empty.toDF(getHeaders:_*)
  // double
//  def createDFFromSeq(tmpSeq: Seq[(String, String, String, Int, Double)]) = tmpSeq.toDF(getHeaders: _*)

  def createDFFromSeq(tmpSeq: Seq[(String, String, String, Int, scala.math.BigDecimal)]) = tmpSeq.toDF(getHeaders: _*)

  def getHeaders = "teamName,sensorName,strValue,intValue,doubleValue".split(",").toSeq

  def getUnionDFWrapper(df: org.apache.spark.sql.DataFrame, rowsSize: Int = 100) = unionDF(df, getRandomSeqOfTuple(rowsSize))
// double
//  def unionDF(df: org.apache.spark.sql.DataFrame, tmpSeq: Seq[(String, String, String, Int, Double)] = getRandomSeqOfTuple()) = df.union(createDFFromSeq(tmpSeq))

  def unionDF(df: org.apache.spark.sql.DataFrame, tmpSeq: Seq[(String, String, String, Int, scala.math.BigDecimal)] = getRandomSeqOfTuple()) = df.union(createDFFromSeq(tmpSeq))

  def inputMapInitializer(args: Array[String]) = for (arg <- args.map(_.split("=", 2)).map(x => (x(0), x(1))))
    inputMap.put(arg._1, arg._2)

  def getValueFromInputMap(key: String) = Try {
    inputMap(key)
  } match {
    case Success(s) => s
    case Failure(f) => {
      println(s"No element in map for key ${key}");
      ""
    }
  }

  def printMapElements = inputMap.map(x => println(s"Value in map for key ${x._1} is value ${x._2}"))

  def dropDupesInDF(numRows: Int) = getRandomDF(numRows).dropDuplicates("teamName", "sensorName")

  def initializeStartDate = controlDateString = getValueFromInputMap("startDate")
  val dateTimeFormat="yyyy-MM-dd"

  val simpleDateFormat = new java.text.SimpleDateFormat(dateTimeFormat)

  def getParsedDate = simpleDateFormat.parse(controlDateString)

  def getDateInMillis = getParsedDate.getTime

//  def addDaysToDate = getDateInMillis + milliSecondsInADay

//  def getFormattedDate = simpleDateFormat.format(addDaysToDate)

//  def addDayToDate:Unit = controlDateString = getFormattedDate

  def addDaysToDate(numDays: Int = 1) = getDateInMillis + (numDays * milliSecondsInADay)

  def getFormattedDate(numDays: Int=1) = simpleDateFormat.format(addDaysToDate(numDays))

  def addDayToDate(numDays: Int=1):Unit = controlDateString = getFormattedDate(numDays)

  def main(args: Array[String]):Unit = {
    // read stream with rate, write with for each batch. Create random DF and then upsert using execute batch (drop duplicates) Do fora date range
    println(s"inside main args ${args.deep}")
    inputMapInitializer(args)
    val insertString = s"insert into ${inputMap("jdbcSchema")}.${inputMap("jdbcTeamSensorTable")}(team_name,sensor_name,str_value,int_value,double_value,start_date) values(?,?,?,?,?,?)"
    val updateString = s"update ${inputMap("jdbcSchema")}.${inputMap("jdbcTeamSensorTable")} set end_date = ? where team_name=? and sensor_name=? and end_date is null "
    println(s"insertString ${insertString} ")
    println(s"updateString ${updateString} ")
    printMapElements
    initializeStartDate
    spark.readStream.format("rate").option("rowsPerSecond", "10").load.writeStream.format("console").option("checkpointLocation", getValueFromInputMap("checkpointLocation")).foreachBatch((df: org.apache.spark.sql.DataFrame, batchID: Long) => {
      forEachBatchFunction(df,batchID,insertString,updateString)
    }).start
    spark.streams.awaitAnyTermination
  }

  def getDFWithControlDate(numRecords: Int = 100) = getRandomDF(numRecords).withColumn("startDate", org.apache.spark.sql.functions.lit(controlDateString))

  def getDFWithControlDateDupesDropped(numRecords: Int = 100) = getDFWithControlDate(numRecords).withColumn("startDate", org.apache.spark.sql.functions.lit(controlDateString))


  def forEachBatchFunction(df: org.apache.spark.sql.DataFrame, batchID: Long,insertString:String,updateString:String) = {
    val reqDF = getDFWithControlDateDupesDropped(20)
  //  @transient val connection = getJDBCConnection
  //  @transient val insertPrepStatement = connection.prepareStatement(insertString)
  //  @transient val updatePrepStatement = connection.prepareStatement(updateString)
    println(s"inside forEachBatchFunction")
    reqDF.as[teamSensorDF].groupByKey(x=>(x.teamName)).flatMapGroups((x,y)=>{
      val connection=getJDBCConnection
      val insertPrepStatement = connection.prepareStatement(insertString)
      val updatePrepStatement = connection.prepareStatement(updateString)
      val listData=y.toList
      println(s"Data for batch ${batchID}, key ${x} and date ${controlDateString} is ${listData}")
      val t= listData.map {
        x =>
          doesRecordExist(x, connection) match {
            case value if value.size == 0 =>
              setInsertBatch(x, insertPrepStatement).addBatch
              x
            case value if value.size > 0 =>
              setUpdateBatch(x, updatePrepStatement).addBatch
              setInsertBatch(x, insertPrepStatement).addBatch
              x
          }
      }
      println(s"updatePrepStatement.executeBatch ${updatePrepStatement.executeBatch.deep}")
      println(s"insertPrepStatement.executeBatch ${insertPrepStatement.executeBatch.deep}")
      connection.close
      t
    }).show(false)
    /*
    reqDF.map(x => {
      doesRecordExist(x, connection) match {
        case value if value.size > 0 =>
          setUpdateBatch(x,updatePrepStatement).addBatch
          setInsertBatch(x,insertPrepStatement).addBatch
          teamSensorTableWithoutEndDate(team_name=x.getAs("team_name").toString,sensor_name=x.getAs("sensor_name").toString,str_value=x.getAs("str_value").toString,int_value=x.getAs("int_value").toString.toInt,double_value=x.getAs("double_value").asInstanceOf[Double],start_date=getStringAsDate(x.getAs("start_date").toString))
        case value if value.size == 0 => // set insert batch
          setInsertBatch(x,insertPrepStatement).addBatch
          teamSensorTableWithoutEndDate(team_name=x.getAs("team_name").toString,sensor_name=x.getAs("sensor_name").toString,str_value=x.getAs("str_value").toString,int_value=x.getAs("int_value").toString.toInt,double_value=x.getAs("double_value").asInstanceOf[Double],start_date=getStringAsDate(x.getAs("start_date").toString))
      }
    }).show(false)
    */
    addDayToDate()
  }

  // "teamName,sensorName,strValue,intValue,doubleValue" and startDate
  def setInsertBatch(incomingRow: org.apache.spark.sql.Row, insertStatement: java.sql.PreparedStatement) = {
    println(s"setInsertBatch incomingRow ${incomingRow}")
    insertStatement.setString(1, incomingRow.getAs("teamName").toString)
    insertStatement.setString(2, incomingRow.getAs("sensorName").toString)
    insertStatement.setString(3, incomingRow.getAs("strValue").toString)
    insertStatement.setInt(4, incomingRow.getAs("intValue").asInstanceOf[Int])
    insertStatement.setDouble(5, incomingRow.getAs("doubleValue").asInstanceOf[Double])
    insertStatement.setDate(5, getStringAsDate(incomingRow.getAs("startDate").asInstanceOf[String]))
    insertStatement
  }

  def setInsertBatch(incomingRow: teamSensorDF, insertStatement: java.sql.PreparedStatement) = {
    println(s"setInsertBatch incomingRow ${incomingRow}")
    insertStatement.setString(1,incomingRow.teamName)
    insertStatement.setString(2,incomingRow.sensorName)
    insertStatement.setString(3,incomingRow.strValue)
    insertStatement.setInt(4,incomingRow.intValue)
    insertStatement.setBigDecimal(5,scalaToJavaBigDecimalConverter(incomingRow.doubleValue))
    insertStatement.setDate(6,getStringAsDate(incomingRow.startDate))
    insertStatement
  }

  def setUpdateBatch(incomingRow: org.apache.spark.sql.Row, updateStatement: java.sql.PreparedStatement)={
    println(s"setUpdateBatch incomingRow ${incomingRow}")
    updateStatement.setDate(1, getStringAsDateMinusDays(incomingRow.getAs("startDate").asInstanceOf[String]))
    updateStatement.setString(2, incomingRow.getAs("teamName").toString)
    updateStatement.setString(3, incomingRow.getAs("sensorName").toString)
    updateStatement
  }

  def setUpdateBatch(incomingRow: teamSensorDF, updateStatement: java.sql.PreparedStatement)={
    println(s"setUpdateBatch incomingRow ${incomingRow}")
    val invalidationDate=getStringAsDateMinusDays(incomingRow.startDate)
    println(s"setUpdateBatch invalidation date ${invalidationDate}")
    updateStatement.setDate(1, invalidationDate)
    updateStatement.setString(2, incomingRow.teamName)
    updateStatement.setString(3, incomingRow.sensorName)
    updateStatement
  }


  def getStringAsDate(dateStr: String) =new java.sql.Date(simpleDateFormat.parse(dateStr).getTime)

  def getStringAsDateMinusDays(dateStr: String,numDays:Int=1) =new java.sql.Date(simpleDateFormat.parse(dateStr).getTime - (numDays * milliSecondsInADay))

  /*
  Agenda -
  Every micro batch contains data in team and sensor level. If the table has records for team and sensor which is active, then invalidate and insert
  else insert.

  Note every micro batch contains 20 rows with select team names and sensor names, but random str ,int and double val. and then a drop dupes occurs on sensor and team to avoid dupes.

  After every batch the start date is incremented by 1

   schema of table

  create table kafka_db.formula_sensor( team_name varchar(25),
   sensor_name varchar(25),
   str_value varchar(25),
   int_value integer(25),
   double_value decimal(26,5),
   start_date date ,
   end_date date default null );
default CURRENT_DATE,


spark-submit --class org.controller.explorations.tmpExploration.scdTypeTwoBatchStart --num-executors 2 --master local[*] --executor-cores 2 --driver-cores 2 --executor-memory 1g --driver-memory 1g --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1g --packages mysql:mysql-connector-java:8.0.23 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkpointLocation="hdfs://localhost:8020/user/raptor/kafka/streams/tmpStream1/" jdbcUser=raptor jdbcURL="jdbc:mysql://localhost:3306/kafka_db?user=raptor?password=" jdbcPassword= jdbcSchema="kafka_db" jdbcTeamSensorTable="formula_sensor" startDate="2020-09-08"
   */
  case class teamSensorTable(team_name: String, sensor_name: String, str_value: String, int_value: Int, double_value: scala.math.BigDecimal, start_date: java.sql.Date, end_date: java.sql.Date = null)
  case class teamSensorTableWithoutEndDate(team_name: String, sensor_name: String, str_value: String, int_value: Int, double_value:  scala.math.BigDecimal, start_date: java.sql.Date)
  case class teamSensorDF(teamName:String,sensorName:String,strValue:String,intValue:Int,doubleValue:scala.math.BigDecimal,startDate:String)
  def getJDBCConnection = java.sql.DriverManager.getConnection(inputMap("jdbcURL"), getJDBCProps)

  def getJDBCProps = {
    val props = new java.util.Properties
    props.put("user", inputMap("jdbcUser"))
    props.put("url", inputMap("jdbcURL"))
    props.put("password", inputMap("jdbcPassword"))
    props
  }

  // teamName,sensorName,strValue,intValue,doubleValue
  def doesRecordExist(incomingRow: org.apache.spark.sql.Row, connection: java.sql.Connection) = {
    val sqlString = s"select * from ${inputMap("jdbcSchema")}.${inputMap("jdbcTeamSensorTable")} where team_name='${incomingRow.getAs("teamName").toString}' and sensor_name='${incomingRow.getAs("sensorName").toString}' and end_date is null"
    println(s"doesRecordExists sqlString ${sqlString}")
    getRecordsForSensorTeam(sqlString, connection)
  }

  def doesRecordExist(incomingRow: teamSensorDF, connection: java.sql.Connection) = {
    val sqlString = s"select * from ${inputMap("jdbcSchema")}.${inputMap("jdbcTeamSensorTable")} where team_name='${incomingRow.teamName}' and sensor_name='${incomingRow.sensorName}' and end_date is null"
    println(s"doesRecordExists sqlString ${sqlString}")
    getRecordsForSensorTeam(sqlString, connection)
  }

  def getRecordsForSensorTeam(sqlString: String, connection: java.sql.Connection) = {
    val tmpArrayBuffer = collection.mutable.ArrayBuffer[teamSensorTable]()
    println(s"getRecordsForSensorTeam sqlString ${sqlString}")
    val resultSet = connection.prepareStatement(sqlString).executeQuery
    while (resultSet.next) {
      val team_name = resultSet.getString(1)
      println(s"team_name=${team_name}")
      val sensor_name = resultSet.getString(2)
      println(s"sensor_name=${sensor_name}")
      val str_value = resultSet.getString(3)
      println(s"str_value=${str_value}")
      val int_value = resultSet.getInt(4)
      println(s"int_value=${int_value}")
      val double_value = javaToScalaBigDecimalConverter(resultSet.getBigDecimal(5))
      println(s"double_value=${double_value}")
      val start_date = resultSet.getDate(6)
      println(s"start_date=${start_date}")
      val end_date = resultSet.getDate(7)
      println(s"end_date=${end_date}")
      tmpArrayBuffer += teamSensorTable(team_name = team_name, sensor_name = sensor_name, str_value = str_value, int_value = int_value, double_value = double_value, start_date = start_date, end_date = end_date)
    }
    println(s"doesRecordExists tmpArrayBuffer ${tmpArrayBuffer}")
    tmpArrayBuffer.toSeq
  }

  def javaToScalaBigDecimalConverter(bigDecimal:java.math.BigDecimal)=new scala.math.BigDecimal(bigDecimal)
  def scalaToJavaBigDecimalConverter(bigDecimal:scala.math.BigDecimal)=new java.math.BigDecimal(bigDecimal.toDouble) // .toString also could work

  // end date is null

  // add all to a batch and execute (update must be executed first , then insert)
  // if record exists set and add update and insert to query.
  // if record does not exist just add it to query



}
