package org.controller.deltaLakeEG

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.constants.projectConstants._
import org.util.SparkOpener
import org.util.readWriteUtil.{getJDBCConnection, javaUtilDateToSqlDateConversion, readDF, readStreamFunction, writeDF, writeStreamConsole}

object readFromDeltaStreamAndPersistInTableDFVersion extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val colSeq="strValue|intValue|dateValue".split("\\|").map(col(_)).toSeq
  def main(args:Array[String]):Unit={
    val inputMap=collection.mutable.Map[String,String]()
    for (arg <- args){
      val keyPart=arg.split("=",2)(0)
      val valPart=arg.split("=",2)(1)
      inputMap.put(keyPart,valPart)
    }
    inputMap.put(fileFormatArg,deltaFormat)
    inputMap.put( filePathArgValue,inputMap("deltaInputPath"))
    val readStreamDF=readStreamFunction(spark,inputMap).select(colSeq:_*)
    inputMap.put(fileTypeArgConstant,fileTypeJDBCValue)
    inputMap.put(driverOption,inputMap("mysqlDriver"))
    inputMap.put(userOption,inputMap("userName"))
    inputMap.put(passwordOption,inputMap("password"))
    inputMap.put(urlOption,inputMap("jdbcURL"))
    inputMap.put(fileOverwriteAppendArg,fileAppendValue)
    inputMap.put(dbtableWriteOption,s"${inputMap("databaseName")}.${inputMap("tableName")}")
    inputMap.put(dbtableReadOption,s"${inputMap("databaseName")}.${inputMap("tableName")}")
    val tempDeltaPersist=readDF(spark, inputMap)

    inputMap.put(fileFormatArg,consoleFormat)
    inputMap.put(outputModeArg,fileAppendValue)
    inputMap.put(checkPointLocationArg,inputMap("checkpointLocation"))
    writeStreamConsole(spark,inputMap,readStreamDF).foreachBatch((dfTemp:DataFrame,batchID:Long) => {
      val dfInfoList=dfTemp.collect
      val strValuesList=listToStringGenerator(dfInfoList.map(_(0).toString).toList)
      val dateValuesList=listToStringGenerator(dfInfoList.map(_(2).toString).toList)
      val requiredRecords=tempDeltaPersist.filter(s"str_value in (${strValuesList}) and date_value in (${dateValuesList})")
      val joinedDF=requiredRecords.as("db").join(dfTemp.withColumn("row_numberCol",row_number.over(Window.partitionBy("strValue","dateValue").orderBy(desc("intValue")))).where("row_numberCol =1").drop("row_numberCol").as("batch"),col("db.str_value")===col("batch.strValue") && col("db.date_value")===col("batch.dateValue"),"full_outer")
      val insertRecords=joinedDF.filter("db.str_value is null").selectExpr("batch.*").select(colSeq:_*)
      val updateRecords=joinedDF.filter("db.str_value is not null and batch.strValue is not null and db.int_value != batch.intValue ").selectExpr("batch.*").select(colSeq:_*)
      val unTouchedRecords=joinedDF.filter("db.str_value is not null and batch.strValue is not null and db.int_value = batch.intValue ").selectExpr("batch.*","db.int_value")
      writeDF(inputMap,insertRecords.selectExpr("strValue as str_value","intValue as int_value","dateValue as date_value"))
     // println(s"Before calling update method in batch ${batchID}")
      updateRecords.map(updateNewIntValues(_,inputMap)).toDF("record","numAffectedRecords").show(false) // .show is to trigger action on the transformation. Needs to return a case class and not a row for encoder issues
      unTouchedRecords.withColumn("unTouchedRecords",org.apache.spark.sql.functions.lit(batchID)).show(false) // for log
    }).start

    spark.streams.awaitAnyTermination

  }
  case class deltaCheck(strValue:String,intValue:Int,dateValue:java.sql.Date)

  def updateNewIntValues(currentRow:org.apache.spark.sql.Row,inputMap:collection.mutable.Map[String,String])={
    val connection=getJDBCConnection(inputMap)
    val updateQuery=s"update ${inputMap("databaseName")}.${inputMap("tableName")} set int_value='${currentRow.getString(1).toInt}',last_updated_timestamp=current_timestamp() where str_value='${currentRow.getString(0)}' and date_value='${currentRow.getString(2)}'"
    println(s"updateQuery - ${updateQuery}")
    val updateStatement=connection.prepareStatement(updateQuery)
    val numRowsAffected=updateStatement.executeUpdate
    updateStatement.closeOnCompletion
    connection.close
    (deltaCheck(currentRow.getString(0),currentRow.getString(1).toInt,javaUtilDateToSqlDateConversion(currentRow.getString(2))),numRowsAffected)
  }
def listToStringGenerator(listOfString:List[String])={
  listOfString.size match{
    case value if value ==0 => "''"
    case value if value >0 =>
      var tmpString=""
      var stringNumCount=1
      for (stringTemp <- listOfString)
        stringNumCount match {
          case value if value ==1 =>
            tmpString = tmpString + s"'${stringTemp}'"
            stringNumCount=stringNumCount+1
          case _ =>
            tmpString = tmpString + s",'${stringTemp}'"
            stringNumCount=stringNumCount+1
        }
      tmpString
  }
}

/*
* This will read from delta stream and then check for the str and date value if record is present in table, if yes updates the int value else it inserts it. IF the int value is same then the record is un touched
*
used DF to join which is good when doing transformations
spark-submit --class org.controller.deltaLakeEG.readFromDeltaStreamAndPersistInTableDFVersion --packages io.delta:delta-core_2.12:0.8.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 --driver-memory 512m --driver-cores 1 --num-executors 1 --executor-memory 512m --executor-cores 1  --master local[*] --conf spark.dynamicAllocation.enabled=false /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootstrapServers=localhost:9092,localhost:9093,localhost:9094 topic=deltaCheckTopic checkpointLocation=hdfs://localhost:8020/user/raptor/streams/deltaReadStream/ deltaInputPath=hdfs://localhost:8020/user/raptor/persist/deltaPersistPath/ startingOffsets=latest mysqlDriver="com.mysql.cj.jdbc.Driver" userName=raptor password= jdbcURL="jdbc:mysql://localhost:3306/testPersist" databaseName=testPersist tableName=delta_persist

*
* */

}
