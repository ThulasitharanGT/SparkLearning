package org.controller

import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants
import org.apache.spark.sql.Row._

object splittingDifferentColumnRecords extends SparkOpener{
  def main(args: Array[String]): Unit = {
    val spark=SparkSessionLoc("splitting diff column records")
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"/Input/multipleRecordSchema.txt")
    inputMap.put(projectConstants.rddPartitionArg,"2")
    val rddFile=readWriteUtil.readRdd(spark,inputMap)
    val rdd5Col=rddFile.map(t =>  if (t.split(projectConstants.delimiterTilde).size ==5 ) t).filter(_.toString.contains(projectConstants.delimiterTilde)).map(_.toString)
    val rdd3Col=rddFile.map(t =>  if (t.split(projectConstants.delimiterTilde).size ==3 ) t).filter(_.toString.contains(projectConstants.delimiterTilde)).map(_.toString)
    // below works in console
    //val df5Col=rdd5Col.toDF("col").selectExpr("split(col,'"+projectConstants.delimiterTilde+"') as col").selectExpr("col[0] as one","col[1] as two","col[2] as three","col[3] as four","col[4] as five")
    //val df3Col=rdd3Col.toDF("col").selectExpr("split(col,'"+projectConstants.delimiterTilde+"') as col").selectExpr("col[0] as one","col[1] as two","col[2] as three")

  }
}
