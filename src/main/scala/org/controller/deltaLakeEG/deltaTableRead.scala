package org.controller.deltaLakeEG

import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants


object deltaTableRead extends SparkOpener {
  val spark=SparkSessionLoc("delta Read")
  def main(args: Array[String]): Unit ={
    val tableName="deltaTablePartitioned"
    val filePath=System.getProperty("user.dir")+projectConstants.pathSep+"Input"+projectConstants.pathSep+tableName
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeDeltaValue)
    inputMap.put(projectConstants.filePathArgValue,filePath)
    //println("==========================================>"+projectConstants.fileTypeDeltaValue)
   // println("==========================================>"+filePath)
    val readDfAsDelta=readWriteUtil.readDF(spark,inputMap)
   // val readDfAsDelta=spark.read.format("delta").load(filePath)
    readDfAsDelta.filter("model = 'Baleno' ").show()
  }

}
