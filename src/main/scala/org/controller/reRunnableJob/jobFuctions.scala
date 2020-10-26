package  org.controller.reRunnableJob
import  org.controller.reRunnableJob.constants._
import  org.controller.reRunnableJob.readWriteUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import sys.process._

object jobFuctions {
  def loadFromRedshift(spark:SparkSession,inputMap:collection.mutable.Map[String,String]) ={
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    inputMap.put(dbTableOption, "(select * from /*red_shiftdb.redshift_table*/)t")
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running from load from redshift\n")
    var df:org.apache.spark.sql.DataFrame=null
    try{
      df=readDF(spark,inputMap)
    }
    catch
      {
        case e:Exception => println(e.printStackTrace)
          fileOutputStream.writeBytes("Error while running load from redshift\n")
          fileOutputStream.close
      }
    df match {
      case null => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        //System.exit(9)
        fileOutputStream.writeBytes("Error while running load from redshift\n")
        fileOutputStream.close
        println("exit")
      }
      case _ => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Load from redshift successful\n")
        fileOutputStream.close
      }
    }
    df
  }
  def columnNameCorrection(df:org.apache.spark.sql.DataFrame)=
  {
    val currentColumns=df.columns.map(_.asInstanceOf[String])
    val correctedColumns=currentColumns/*map(x => x.substring(0,x.indexOf("(")))*/.map(_.trim).map(_.replace(" ","_"))
    df.toDF(correctedColumns:_*)
  }

  def loadFromHttp (spark:SparkSession,inputMap:collection.mutable.Map[String,String]) ={
    val colNames="Element,Number,Symbol,Weight,Boil,Melt,density_Vapour,fusion,diffusion_index".split(",").map(_.asInstanceOf[String])
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running from load from http\n")

    wgetRunner(inputMap) match// if the result is false exit
    {
      case false => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        //System.exit(9)
        fileOutputStream.writeBytes("Error while downloading file from http\n")
        fileOutputStream.close
        println("exit")
      }
      case true =>         fileOutputStream.writeBytes("Downloaded file successfully from http\n")
    }
    //overwriting configs for http here

    inputMap.put(pathOption,s"hdfs:///user/raptor/hadoop/data/data_${inputMap(jobRunIdArg)}.csv")
    s"hdfs dfs -put /home/raptor/temp/fileDownload/data_${inputMap(jobRunIdArg)}.csv hdfs:///user/raptor/hadoop/data/data_${inputMap(jobRunIdArg)}.csv"!
   // dbutils.fs.cp(inputMap(wgetFileNameArg),inputMap(filePathArg))
    var df:org.apache.spark.sql.DataFrame=null
    try{
      //df=columnNameCorrection(readDF(spark,inputMap))
      df=readDF(spark,inputMap).toDF(colNames:_*)
    }
    catch
      {
        case e:Exception => println(e.printStackTrace)
          fileOutputStream.writeBytes("Error while running load from http\n")
          fileOutputStream.close
      }
    df match {
      case null => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        //System.exit(9)
        fileOutputStream.writeBytes("Error while running load from http\n")
        fileOutputStream.close
        println("exit")
      }
      case _ => {
        inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
        execSparkSql(spark,inputMap)
        fileOutputStream.writeBytes("Load from http successful\n")
        fileOutputStream.close
      }
    }
    df
  }

  def archivalForRedshift(dfTemp:org.apache.spark.sql.DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String])={
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    inputMap.put(DFrepartitionArg, "2")
    inputMap.put(saveModeArg, "overwrite")
    inputMap.put(tableNameArg, "tempDb.temp_pipeline_archive")
    fileOutputStream.writeBytes("Running Archive redshift data in temp table\n ")
    try{
     //insertIntoHive(spark,inputMap,dfTemp.repartition(10))
      saveAsTableHive(spark,inputMap,dfTemp.repartition(10))
    }
    catch
      {
        case e:Exception =>
        {
          println(e.printStackTrace)
          inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          fileOutputStream.writeBytes("Archive redshift data in temp table failed\n")
          fileOutputStream.close
          //System.exit(9)
          println("exit")
        }
      }
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${successMessage}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    fileOutputStream.writeBytes("Archive redshift data in temp table Successful\n")
    fileOutputStream.close
  }

  def insertDeltaToHive(dfTemp:org.apache.spark.sql.DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String]) =
  {
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running insert into hive table \n")
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    inputMap.put(DFrepartitionArg, "2")
    inputMap.put(saveModeArg, "append")
    inputMap.put(tableNameArg,"tempDb.pipeline_temp_1")
    try{
      insertIntoHive(spark,inputMap ,dfTemp.withColumn("dataDate",lit(inputMap(runDateArg))).repartition(10))
    }
    catch
      {
        case e:Exception => {
          println("failed to write into HiveBase Table")
          inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          fileOutputStream.writeBytes("insert into hive table failed\n")
          fileOutputStream.close
          //System.exit(9)
          println("exit")
        }
      }
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    fileOutputStream.writeBytes("insert into hive table success\n")
    fileOutputStream.close
  }

  def snapShotHive(dfTemp:org.apache.spark.sql.DataFrame,spark:SparkSession,inputMap:collection.mutable.Map[String,String])=
  {
    val fileOutputStream=fileOutputStreamObjectCreator(inputMap(hdfsDomain), inputMap(logPathArg))
    fileOutputStream.writeBytes("Running snapshot into hive table \n")
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${runningString}',${inputMap(jobRunIdArg)})") // change run date
    inputMap.put(DFrepartitionArg, "2")
    inputMap.put(saveModeArg, "append")
    inputMap.put(tableNameArg,"tempDb.pipeline_temp_2") // if you need dynamic table , name read it from a file and pass it dynamically from main funvtion like you do in count or data scheck
    try{
      insertIntoHive(spark,inputMap ,dfTemp.withColumn("dataDate",lit(inputMap(runDateArg))).withColumn("jobRunID",lit(inputMap(jobRunIdArg))).repartition(10))
    }
    catch
      {
        case e:Exception => {
          println("failed to write into hive archive Table")
          inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${failedString}',${inputMap(jobRunIdArg)})") // change run date
          execSparkSql(spark,inputMap)
          fileOutputStream.writeBytes("snapshot into hive table failed\n")
          fileOutputStream.close
          //System.exit(9)
          println("exit")
        }
      }
    inputMap.put(sqlStringArg, s"insert into tempDb.temp_audit_table values('${inputMap(mainJobNameArg)}','${inputMap(currentJobNameArg)}',current_timestamp(),current_timestamp(),'${inputMap(runDateArg)}','${completedString}',${inputMap(jobRunIdArg)})") // change run date
    execSparkSql(spark,inputMap)
    fileOutputStream.writeBytes("snapshot into hive table success\n")
    fileOutputStream.close
  }
}
