package org.controller.deltaLakeEG

import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants

object deltaLakeNewColumnTableTry extends SparkOpener{
  def main(args: Array[String]): Unit = {
    val spark=SparkSessionLoc("delta lake hadoop")
    val basePath="hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/"
    val inputPath=basePath+"inputFiles/"
    val outputPath=basePath+"outputFiles/"
    val tableBronzeName="carsDeltaTableAddingColumnInBetween_Silver"
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.basePathArgConstant,basePath)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car2.txt")
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
   // val deltaInput1=readWriteUtil.readDF(spark,inputMap)
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car3.txt")
   // val deltaInput2=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car4.txt")
   //val deltaInput3=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car_ExtraColumn_schema.txt")
    val deltaInputDiffSchema=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time","number_of_owners")

    deltaInputDiffSchema.write.mode("overwrite").format("delta")/*.option("overwriteSchema", "true")*/.partitionBy("brand","model","year","month").save(outputPath+tableBronzeName)


  }


}
