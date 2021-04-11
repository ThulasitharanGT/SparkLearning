package org.controller.persistingInsideJob

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.DriverManager
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
    Class.forName(inputMap("driverMYSQL"))
    val props=new Properties
    props.put("user",inputMap("username"))
    props.put("password",inputMap("password"))
    props.put("url",inputMap("urlJDBC"))
    val connectionInstance=DriverManager.getConnection(inputMap("urlJDBC"),props)
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
}
