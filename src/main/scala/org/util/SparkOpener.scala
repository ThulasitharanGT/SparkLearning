package org.util

//import org.constants.PathConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.concurrent.ThreadLocalRandom

trait SparkOpener
{
  val stringList=(('a' to 'z') ++('A' to 'Z')).map(_.toString)
  val numberList= 0 to Int.MaxValue-1

  def stringGenerator(lengthOfString:Int) = {
    var outputString=""
    for (i <- 1 to lengthOfString)
      outputString=outputString+stringList(ThreadLocalRandom.current.nextInt(0,stringList.size-1))
    outputString
  }

  def numberGenerator(minValue:Int=0,maxValue:Int=Int.MaxValue)=numberList(ThreadLocalRandom.current.nextInt(minValue,maxValue-1))

  def randomNameGenerator = s"${stringGenerator(ThreadLocalRandom.current.nextInt(5,10))}_${numberGenerator(0,Int.MaxValue)}"
  def SparkSessionLoc(name:String =randomNameGenerator):SparkSession={
    val conf=new SparkConf().setAppName(name +"Local" ).setMaster("local")
    conf.set("spark.sql.parquet.binaryAsString","true").set("spark.sql.avro.binaryAsString","true")
    //conf.set("spark.testing.memory","671859200").set("spark.ui.enabled","true").set("spark.sql.parquet.binaryAsString","true").set("spark.sql.avro.binaryAsString","true")
    //System.setProperty("hadoop.home.dir",PathConstants.WINUTILS_EXE_PATH)
    SparkSession.builder().config(conf).getOrCreate()
    }
}
