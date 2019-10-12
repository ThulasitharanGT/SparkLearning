package org.controller

import org.util.SparkOpener

object scDotTextFileMethodOfFirstAndLastLine extends SparkOpener{

  //taking first and last line of a text file which is loaded by sc.textFile()
def main(args:Array[String]) ={
    val spark=SparkSessionLoc("testScDotTextFile")
    val sc=spark.sparkContext
    val fileRdd=sc.textFile(System.getProperty("user.dir")+"\\Input\\dept.txt")
    val totNumOfLines=(fileRdd.count).toInt
    // first line
    val firstLine= fileRdd.take(1)
    // all lines line as array ofstring
    val lastLine= fileRdd.take(totNumOfLines)
    // array of first and last line
    val firstAndLastLine:Array[String] = Array(firstLine(0),lastLine(totNumOfLines-1))
    firstAndLastLine.foreach(print(_))
  }
}
