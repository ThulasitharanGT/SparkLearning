package org.controller

import org.constants.projectConstants
import org.util.{SparkOpener, readWriteUtil}

object explodeExample extends SparkOpener {
  val spark=SparkSessionLoc("explode try")
def main (args:Array[String]):Unit ={
  //all the columns in a single row
  val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
  for (arg <- args)
       {
         val keyPart=arg.split("=",2)(0)
         val valPart=arg.split("=",2)(1)
         inputMap.put(keyPart,valPart)
       }
  val basePath=inputMap("basePath")  //file:///home/raptor/IdeaProjects/SparkLearning/Input/
  val fileName=inputMap("fileName")  //multipleRecordSchema.txt
  inputMap.put(projectConstants.fileTypeArgConstant,projectConstants.fileTypeCsvValue)
  inputMap.put(projectConstants.fileFormatArg,projectConstants.csvFormat)
  inputMap.put(projectConstants.delimiterArgConstant,projectConstants.delimiterComma)
  inputMap.put(projectConstants.inferSchemaArgConstant,projectConstants.stringTrue)
  inputMap.put(projectConstants.headerArgConstant,projectConstants.stringFalse)
  inputMap.put(projectConstants.basePathArgConstant,basePath)
  inputMap.put(projectConstants.filePathArgValue,basePath+fileName)
  val df=readWriteUtil.readDF(spark,inputMap)
  val dfExploded=df.selectExpr("explode(split(_c0,'~')) as col")
  dfExploded.show(false)
}
  /*
  cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

spark-submit --class org.controller.explodeExample --driver-cores 2 --driver-memory 1g --executor-memory 1g --num-executors 2 --executor-cores 2  SparkLearning-1.0-SNAPSHOT.jar basePath=file:///home/raptor/IdeaProjects/SparkLearning/Input/  fileName=multipleRecordSchema.txt

   */

}
