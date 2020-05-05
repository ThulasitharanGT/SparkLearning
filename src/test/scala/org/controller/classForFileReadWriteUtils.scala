package org.controller
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit._
import org.junit.Assert._
import org.util.{SparkOpener, readWriteUtil}
import org.constants.projectConstants._

class classForFileReadWriteUtils extends SparkOpener {
  /*
  val spark=SparkSessionLoc("forJunit")
  import spark.implicits._
  var defaultDF:DataFrame=null
  val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
  @Before
  def startingInitialization(): Unit =
  {
    defaultDF=Array(("invalid Scn"," Error")).toSeq.toDF("Scn","Desc")
  }

  @Test
  def dataframeReturnChecker():Unit=
  {
    inputMap.put(fileFormatArg,"na")
    assertEquals(readWriteUtil.readDF(spark,inputMap),defaultDF)
  }
*/

}
