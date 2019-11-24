package org.controller.deltaLakeEG
import io.delta.tables._
import org.constants.projectConstants
import org.util.{SparkOpener,readWriteUtil}

object deltaLakeHadoopEg extends SparkOpener {
  def main(args: Array[String]): Unit = {
    val spark=SparkSessionLoc("delta lake hadoop")


    val basePath="hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/"
    val inputPath=basePath+"inputFiles/"
    val outputPath=basePath+"outputFiles/"
    val tableBronzeName="carsDeltaTable_Bronze"
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
    inputMap.put(projectConstants.basePathArgConstant,basePath)
    inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car2.txt")
    inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
    inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterOr)
    inputMap.put(projectConstants.headerArgConstant,projectConstants.stringTrue)
     val deltaInput1=readWriteUtil.readDF(spark,inputMap)
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car3.txt")
    val deltaInput2=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car4.txt")
    val deltaInput3=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
    inputMap.put(projectConstants.filePathArgValue,inputPath+"Avail_car_ExtraColumn_schema.txt")
  // -->  val deltaInputDiffSchema=readWriteUtil.readDF(spark,inputMap).selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time","number_of_owners")
   // readWriteUtil.writeDF(inputMap,deltaInput1) // cant partition in this method
    deltaInput1.write.mode("overwrite").format("delta").partitionBy("brand","model","year","month").save(outputPath+tableBronzeName)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeDeltaValue)
    inputMap.put(projectConstants.fileOverwriteAppendArg,projectConstants.fileAppendValue)
    inputMap.put(projectConstants.filePathArgValue,outputPath+tableBronzeName+projectConstants.pathSep)
    readWriteUtil.writeDF(inputMap,deltaInput2)
   // over writing existing partitions which are present in DF alone
   // spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    deltaInput3.write.mode("overwrite").option("spark.sql.sources.partitionOverwriteMode","dynamic").format("delta").save(outputPath+tableBronzeName)
    val deltaTable=DeltaTable.forPath(spark,outputPath+tableBronzeName)
    deltaTable.toDF.select("brand","model").distinct.show(100,false)
    // -->deltaInputDiffSchema.write.mode("append").option("mergeSchema","true").format("delta").save(outputPath+tableBronzeName)
    deltaTable.toDF.select("brand","model").distinct.show(100,false)
    deltaTable.history.show(false)
    spark.stop()
//cd  /home/raptor/IdeaProjects/SparkLearning/build/libs/
//spark-submit --class org.controller.deltaLakeEG.deltaLakeHadoopEg --master yarn --deploy-mode client --packages io.delta:delta-core_2.11:0.4.0  SparkLearning-1.0-SNAPSHOT.jar

    // val deltaTableInput2=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car2.txt")
   // val deltaTableInput3=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car4.txt")
   //  val tempDeltaTableInput1= deltaTableInput1.selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time")
   // deltaTableInput2.write.mode("overwrite").option("spark.sql.sources.partitionOverwriteMode","dynamic").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
    //tempDeltaTableInput1.write.mode("overwrite").partitionBy("brand","model","year","month").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/deltaTablePartitioned_bronze")
   // val deltaTableDeltaFormat=DeltaTable.forPath(spark,"/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
    //deltaTableInput3.write.mode("append").option("spark.sql.sources.partitionOverwriteMode","dynamic").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/temp_DeltaTablePartitioned")
   // deltaTableDeltaFormat.updateExpr("model = 'Eco-Sport'",Map("model"->"'Free-Style'","year"->"2017")) // string in single quoutes in update expr
    //deltaTableDeltaFormat.as("HIST").merge(deltaTableInput1.as("CURR"),"HIST.Vehicle_id = CURR.Vehicle_id").whenMatched().updateAll().whenNotMatched().insertAll().execute()
   // tempDeltaTableInput1.show
    //val deltaTableInputExtraColumn=spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter","|").option("inferSchema","true").load("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/inputFiles/Avail_car_ExtraColumn_schema.txt").selectExpr("Vehicle_id","model","brand","year","month","miles","CAST(concat(substring(intake_date_time,7,4),concat(substring(intake_date_time,3,4),concat(substring(intake_date_time,1,2),substring(intake_date_time,11,9)))) AS TIMESTAMP) as intake_date_time","number_of_owners")
   // deltaTableInputExtraColumn.write.mode("overwrite").partitionBy("brand","model","year","month").option("spark.sql.sources.partitionOverwriteMode","dynamic").option("mergeSchema","true").format("delta").save("hdfs://localhost/user/raptor/testing/hadoop/deltaTableTestFolder/deltaTablePartitioned_broze_extraColumn")
  }
}
