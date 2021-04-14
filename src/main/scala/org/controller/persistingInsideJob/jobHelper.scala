package org.controller.persistingInsideJob

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.controller.persistingInsideJob.streamToStaticDF.statsTable

import java.sql.{Connection, DriverManager}
import java.util.Properties
import scala.util.Try

object jobHelper {
  def argsToMapConvert(commandLineArguments:Array[String])={
    val inputMap=collection.mutable.Map[String,String]()
    for (commandLineArgument <- commandLineArguments)
    {
      val keyPart=commandLineArgument.split("=",2)(0)
      val valPart=commandLineArgument.split("=",2)(1)
      inputMap.put(keyPart,valPart)
    }
    inputMap
  }

  def idGetterDriver(df:DataFrame)=idGetter(df,1)
  def idGetterTeam(df:DataFrame)=idGetter(df,0)

  def idGetter(df:DataFrame,indexNum:Int):List[String]={
    var tmpArrayBuffer=collection.mutable.ArrayBuffer[String]()
    // println(s"inside idGetter")
    df.collect.map(x => {tmpArrayBuffer += x(indexNum).toString})
    tmpArrayBuffer.toList.distinct
  }

  def idListToStringManipulator(idList:List[String])={
    var incomingIDConditionString=""
    var elementNum=1
    idList.map(x => elementNum match { case value if value ==1 => incomingIDConditionString =incomingIDConditionString + s"'${x}'" ; elementNum=elementNum+1 case _ => incomingIDConditionString =incomingIDConditionString + s",'${x}'" ; elementNum=elementNum+1 })
    incomingIDConditionString.trim.size match {case value if value >0 => incomingIDConditionString case _ => "''"}
  }

  def getExistingRecords(inputMap:collection.mutable.Map[String,String],spark:SparkSession):DataFrame={
    spark.read.format("jdbc").option("driver",inputMap("driverMYSQL")).option("user",inputMap("username")).option("password",inputMap("password")).option("url",inputMap("urlJDBC")).option("dbtable",inputMap("queryString")).load
  }
  def getExistingRecords(inputMap:collection.mutable.Map[String,String],spark:SparkSession,query:String):DataFrame={
    spark.read.format("jdbc").option("driver",inputMap("driverMYSQL")).option("user",inputMap("username")).option("password",inputMap("password")).option("url",inputMap("urlJDBC")).option("dbtable",/*inputMap("queryString")*/query).load
  }

  def writeToTable(df:DataFrame,inputMap:collection.mutable.Map[String,String],tableName:String)=df.write.mode("append").format("jdbc").option("driver",inputMap("driverMYSQL")).option("user",inputMap("username")).option("password",inputMap("password")).option("url",inputMap("urlJDBC")).option("dbtable",/*inputMap("queryString")*/s"${inputMap("databaseName")}.${tableName}").save

  def updateExistingValidRecords(inputMap:collection.mutable.Map[String,String]) ={
/*    Class.forName(inputMap("driverMYSQL"))
    val props=new Properties
    props.put("user",inputMap("username"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("urlJDBC"))*/
    val connectionInstance=getJDBCConnection(inputMap)
    val query=inputMap("sqlQuery")
    println(s"update query ${query}")
    val preparedStatement=connectionInstance.prepareStatement(query)
    val rowsAffected=preparedStatement.executeUpdate
    connectionInstance.close
    rowsAffected
  }

  def expiryCheck(df:DataFrame,inputMap:collection.mutable.Map[String,String]) ={
    val stateInfoDF=df.withColumn("currentTime",lit(current_timestamp)).withColumn("plusMinutes",col("receivedTimestamp")+expr(inputMap("stateExpiry"))).withColumn("minusMinutes",col("currentTime")-expr(inputMap("stateExpiry")))
    val expiredDF=stateInfoDF.where("plusMinutes <= currentTime")
    val retainedDF=stateInfoDF.where("minusMinutes <= receivedTimestamp")
    (retainedDF,expiredDF)
  }

  def updateTeamTable(inputMap:collection.mutable.Map[String,String],df:DataFrame)={
    val teamIds=idGetterTeam(df)
    val teamListString=idListToStringManipulator(teamIds)
    val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("teamTableName")} set delete_timestamp=now(),team_valid_flag='N' where team_id in (${teamListString})"
    val conn=getJDBCConnection(inputMap)
/*    val updateStatement=conn.prepareStatement(updateQuery)
    val recordsUpdated=updateStatement.executeUpdate*/
    val recordsUpdated =executeUpdateAndReturnRecordsAffected(conn,updateQuery)
    conn.close
    recordsUpdated
  }

  def updateDriverTable(inputMap:collection.mutable.Map[String,String],df:DataFrame)={
    val driverIds=idGetterDriver(df)
    val driverListString=idListToStringManipulator(driverIds)
    val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("driverTableName")} set delete_timestamp=now(),active_flag='N' where driver_id in (${driverListString})"
    val conn=getJDBCConnection(inputMap)
    /*    val updateStatement=conn.prepareStatement(updateQuery)
        val recordsUpdated=updateStatement.executeUpdate*/
    val recordsUpdated =executeUpdateAndReturnRecordsAffected(conn,updateQuery)
    conn.close
    recordsUpdated
  }

  def getJDBCConnection(inputMap:collection.mutable.Map[String,String])={
    Class.forName(inputMap("driverMYSQL"))
    val props=new Properties
    props.put("user",inputMap("username"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("urlJDBC"))
    DriverManager.getConnection(inputMap("urlJDBC"),props)
  }
  def executeUpdateAndReturnRecordsAffected(conn:Connection,query:String)={
    val updateStatement=conn.prepareStatement(query)
    val recordsUpdated=updateStatement.executeUpdate
    recordsUpdated
  }


  def streamingDFToStaticDF(df:Dataset[statsTable],spark:SparkSession)={
    import spark.implicits._
    var staticDFTemp:DataFrame=null
    val StreamingDF=df.map(x=>
      staticDFTemp match {
        case null =>
          staticDFTemp=Seq(x).toDF
          x
        case value =>
          staticDFTemp=value.union(Seq(x).toDF)
          x
      }
    )
    (staticDFTemp,StreamingDF)
  }

  def capitalize(str:String) =Try{str.size} match {
    case value if value.isSuccess == true => s"${str.substring(0,1).toUpperCase}${str.substring(1)}"
    case value if value.isFailure == true => s"String was NULL"
  }

  def listToCamelCaseStringGenerator(varNameList:Array[String])={
    val remainingArray=varNameList.diff(varNameList.head)
    s"${varNameList.head}${capitalizedStringGenerator(remainingArray.toList)}"
  }
  def capitalizedStringGenerator(camelCaseStringList:List[String])={
    var secondHalfVariable=""
    camelCaseStringList.map(x=> secondHalfVariable=secondHalfVariable+capitalize(x))
    secondHalfVariable
  }

}
