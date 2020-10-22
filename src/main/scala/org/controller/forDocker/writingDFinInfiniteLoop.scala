package org.controller.forDocker

import java.util.concurrent.ThreadLocalRandom

import org.util.SparkOpener

object writingDFinInfiniteLoop extends SparkOpener{
  val spark=SparkSessionLoc("dfWrite")
  val listOfChars = ('a' to 'z') ++ ('A' to 'Z')
  def randomStringGenerator(tempStringLength: Int) = {
    var tempString: String = null
    for (i <- 1 to tempStringLength)
      if (tempString == null)
        tempString = listOfChars(ThreadLocalRandom.current().nextInt(0, listOfChars.size)).toString
      else
        tempString = tempString + listOfChars(ThreadLocalRandom.current().nextInt(0, listOfChars.size)).toString
    tempString
  }
  val listOfNumbers = 1 to 1000000
  def randomNumber=listOfNumbers(ThreadLocalRandom.current().nextInt(0, listOfNumbers.size)).toLong
  def main(args:Array[String]):Unit={
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for (arg <- args)
      {
        val keyPart=arg.split("=",2)(0)
        val argPart=arg.split("=",2)(1)
        inputMap.put(keyPart,argPart)
      }
    import spark.implicits._
    //val outputPath=inputMap("outputPath")
    while(true)
      Seq(dataFrameInfiniteLoop(randomStringGenerator(1000),randomNumber)).toDF.show(false)
      //Seq(dataFrameInfiniteLoop(randomStringGenerator(1000),randomNumber)).toDF.write.mode("append").save(outputPath)

  }
//spark-submit --class org.controller.forDocker.writingDFinInfiniteLoop --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1 --deploy-mode client --master yarn /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar outputPath=hdfs://localhost/user/raptor/testing
}
