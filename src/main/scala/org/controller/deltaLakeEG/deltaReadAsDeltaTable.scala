package org.controller.deltaLakeEG

import org.constants.projectConstants
import org.util.{SparkOpener,readWriteUtil}
import io.delta.tables._
import org.apache.spark.sql.functions._

object deltaReadAsDeltaTable extends SparkOpener{
  val spark=SparkSessionLoc("delta read as delta table")
  def main(args: Array[String]): Unit = {
    val tableName="deltaTablePartitioned"
    val basePath=System.getProperty("user.dir")+projectConstants.pathSep+"Input"
    val filePath=basePath+projectConstants.pathSep+tableName
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.filePathArgValue,filePath)
  //  val deltaTableTemp=DeltaTable.forPath(spark,filePath) - this is the original syntax
    val deltaTableTemp=readWriteUtil.deltaTableRead(spark,inputMap)
    val mergeFilePath=basePath+projectConstants.pathSep+"Avail_cars2.txt"
    inputMap.put(projectConstants.filePathArgValue,mergeFilePath)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.basePathArgConstant,basePath)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
    val df =readWriteUtil.readDF(spark,inputMap)
    deltaTableTemp.update(col("model") === "Endeavour",Map("model"->lit("FreeStyle"),"miles"->lit(4000)))
    // or
   // deltaTableTemp.updateExpr("model = 'Endeavour'",Map("model"->"FreeStyle","miles"->"4000"))
    // folder new one is created from endeavour to free style. new data wll read from free style  as updated in log
    deltaTableTemp.delete("brand = 'Fiat' ")

   // Upserts all
  // deltaTableTemp.as("HIST").merge(df.as("CURR"),"HIST.Vehicle_id = CURR.Vehicle_id").whenMatched().updateAll().whenNotMatched().insertAll().execute()

    // wanted column alone
    deltaTableTemp.as("HIST").merge(df.as("CURR"),"HIST.Vehicle_id = CURR.Vehicle_id").whenMatched().updateExpr(Map("HIST.month" -> "CURR.month","HIST.miles" -> "CURR.miles","HIST.intake_date_time" -> "CURR.intake_date_time","HIST.year" -> "CURR.year","HIST.brand" -> "CURR.brand","HIST.model" -> "CURR.model")).whenNotMatched().insertAll().execute()

     deltaTableTemp.toDF.show()
  }

}
