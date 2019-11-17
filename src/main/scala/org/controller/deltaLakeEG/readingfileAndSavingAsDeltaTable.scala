package org.controller.deltaLakeEG

import org.constants.projectConstants
import org.util.SparkOpener
import org.util.readWriteUtil

object readingfileAndSavingAsDeltaTable extends SparkOpener{

  val spark=SparkSessionLoc("temp")
  def main(args: Array[String]): Unit = {
    val inputMap=collection.mutable.Map[String,String]()
    val tableName="tablePartitioned"
    val deltaTable="tablePartitioned_delta"
    val basePath=System.getProperty("user.dir")+projectConstants.pathSep+"Input"+projectConstants.pathSep+tableName
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeParquetValue)
    inputMap.put(projectConstants.basePathArgConstant,basePath)
    inputMap.put(projectConstants.filePathArgValue,basePath)
    val df=readWriteUtil.readDF(spark,inputMap)
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeDeltaValue)
    inputMap.put(projectConstants.filePathArgValue,basePath.replace(tableName,deltaTable))
    //readWriteUtil.writeDF(inputMap,df)
    df.write.mode("overwrite").format("delta").partitionBy("year","brand","model")/*.sortBy("Vehicle_id").bucketBy(20,"Vehicle_id")*/.save(basePath.replace(tableName,"deltaTablePartitioned"))


  }

}
