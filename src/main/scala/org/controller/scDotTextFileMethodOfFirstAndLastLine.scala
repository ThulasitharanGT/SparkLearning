package org.controller

import org.apache.spark.ml.feature.NGram
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
    firstAndLastLine.foreach(println(_))
    /// ngram
    val dfTemp=spark.createDataFrame(Seq((1,"cool buddy life"),(2,"life is hectic"),(3,"life plays us"))).toDF("sno","words")
    val dfTempTransformed=dfTemp.selectExpr("sno","split(words,' ') as wordsT")
    val ngram=new NGram().setInputCol("wordsT").setOutputCol("ngram")
    val newDf=ngram.transform(dfTempTransformed)
    newDf.show(false)
    dfTemp.selectExpr("explode(split(words,' '))").show(false)
    // factorial
    val range=1 to 5
    println(range.product)
    //divisibility  check
    range.groupBy(_%3)// returns map (remainder ,numbers which get this remainder) any number can be used
  }
}
