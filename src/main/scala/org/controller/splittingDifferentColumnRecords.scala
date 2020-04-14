package org.controller

import org.util.{SparkOpener,readWriteUtil}
import org.constants.projectConstants

object splittingDifferentColumnRecords extends SparkOpener{
  def main(args: Array[String]): Unit = {
    val spark=SparkSessionLoc("splitting diff column records")
    val inputMap=collection.mutable.Map[String,String]()
    inputMap.put(projectConstants.filePathArgValue,System.getProperty("user.dir")+"/Input/multipleRecordSchema.txt")
    inputMap.put(projectConstants.rddPartitionArg,"2")
    import spark.implicits._
    val rddFile=readWriteUtil.readRdd(spark,inputMap)
    val df5Col=rddFile.map(t => t.split(projectConstants.delimiterTilde)).filter(_.size ==5).toDF("col").selectExpr("col[0] as one","col[1] as two","col[2] as three","col[3] as four","col[4] as five")
    val df3Col=rddFile.map(t => t.split(projectConstants.delimiterTilde)).filter(_.size ==3).toDF("col").selectExpr("col[0] as one","col[1] as two","col[2] as three")
    df5Col.show(false)
    df3Col.show(false)

    /*

    around the head
    val df5Col=rddFile.map(t =>  if (t.split(projectConstants.delimiterTilde).size ==5 ) t).filter(_.toString.contains(projectConstants.delimiterTilde)).map(_.toString).toDF("col").selectExpr("split(col,'"+projectConstants.delimiterTilde+"') as col").selectExpr("col[0] as one","col[1] as two","col[2] as three","col[3] as four","col[4] as five")
    val df3Col=rddFile.map(t =>  if (t.split(projectConstants.delimiterTilde).size ==3 ) t).filter(_.toString.contains(projectConstants.delimiterTilde)).map(_.toString).toDF("col").selectExpr("split(col,'"+projectConstants.delimiterTilde+"') as col").selectExpr("col[0] as one","col[1] as two","col[2] as three")

   */
    /*
    val df=spark.read.csv("file:///home/raptor/IdeaProjects/SparkLearning/Input/multipleRecordSchema.txt")
    val dfWithArray=df.selectExpr("split(_c0,'~') as colArray","_c0")
    val dfWithArraySize=dfWithArray.selectExpr("size(colArray) as tempSize","colArray","_c0")
    //dfWithArray.select(size(col("colArray"))).show
    val df5col=dfWithArraySize.filter("tempSize =5").select("colArray").selectExpr("colArray[0] as one","colArray[1] as two","colArray[2] as three","colArray[3] as four","colArray[4] as five")
    val df3col=dfWithArraySize.filter("tempSize =3").select("colArray").selectExpr("colArray[0] as one","colArray[1] as two","colArray[2] as three")
-------------

second try
val dfWithArray=df.withColumnRenamed("_c0","col").selectExpr("split(col,'~') as colArray").selectExpr("size(colArray) as numCols","colArray")

dfWithArray.filter("numCols =5").selectExpr("colArray[0] as one").selectExpr("colArray[1] as two").selectExpr("colArray[2] as three").selectExpr("colArray[3] as four").selectExpr("colArray[4] as five")

dfWithArray.filter("numCols =3").selectExpr("colArray[0] as one").selectExpr("colArray[1] as two").selectExpr("colArray[2] as three")

    -------------
    third RDD try
    inputMap.put(projectConstants.filePathArgValue,"file:///user/raptor/IdeaProjects/SparkLearning/Input/multipleRecordSchema.txt") 
    inputMap.put(projectConstants.rddPartitionArg,"2")
    val rddTemp=sc.textFile(inputMap(projectConstants.filePathArgValue),inputMap(projectConstants.rddPartitionArg).toInt)
    val cols3DF=rddTemp.map(x=> x.split("~")).filter(_.size ==3).toDF("arrayValue").selectExpr("arrayValue[0] as firstColumn","arrayValue[1] as secondColumn","arrayValue[2] as thirdColumn")
    val cols5DF=rddTemp.map(x=> x.split("~")).filter(_.size ==5).toDF("arrayValue").selectExpr("arrayValue[0] as firstColumn","arrayValue[1] as secondColumn","arrayValue[2] as thirdColumn","arrayValue[3] as fourthColumn","arrayValue[4] as fifthColumn")

    ---------------


*/
  }
}
