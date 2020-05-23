package org.controller.deltaLakeEG
import java.util.concurrent.ThreadLocalRandom
import org.apache.spark.sql.functions._
import org.util.SparkOpener
//org.controller.deltaLakeEG.streamUsingRateExample
object streamUsingRateExample extends SparkOpener {
  val spark=SparkSessionLoc("temp")
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
  def main(args:Array[String]):Unit =
    {
      val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
      for (arg <- args)
        inputMap.put(arg.split("=",2)(0),arg.split("=",2)(1))
      val checkPointPath=inputMap("checkPointPath")
      val outputPath=inputMap("outputPath")
      val snapshotDateInfo=inputMap("snapshotDateInfo")
      val snapshotDate=inputMap("snapshotDate")
      var snapshotDateCurrent:String=null
      snapshotDateInfo.toLowerCase match
        {
        case "today" => snapshotDateCurrent=org.joda.time.LocalDate.now.toString
        case "before" => snapshotDateCurrent=org.joda.time.LocalDate.now.minusDays(snapshotDate.toInt).toString
        case "after" => snapshotDateCurrent=org.joda.time.LocalDate.now.plusDays(snapshotDate.toInt).toString
        case _=> println("invalid option")
       }
      val streamDF=spark.readStream.format("rate").option("rowsPerSecond",5).load.withColumn("stringValue",lit(randomStringGenerator(100))).withColumn("intValue",lit(randomNumber)).withColumn("SnapshotDate",lit(snapshotDateCurrent))
      //val query=streamDF.writeStream.format("delta").option("mergeSchema","true").option("checkpointLocation",checkPointPath).option("maxFilesPerTrigger","2").option("path",outputPath).start
     val query=streamDF.writeStream.format("parquet").option("checkpointLocation",checkPointPath).option("maxFilesPerTrigger","2").option("path",outputPath).start
       query.awaitTermination
//format delta throwing error for docker
      /*
      //first stream
spark-submit --class org.controller.deltaLakeEG.streamUsingRateExample --packages io.delta:delta-core_2.11:0.5.0 --num-executors 1 --executor-memory 1g --driver-cores 1 --driver-memory 1g --deploy-mode client --master yarn /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkPointPath=hdfs://localhost/user/raptor/kafka/stream/checkPoint1RateTry/ outputPath=hdfs://localhost/user/raptor/kafka/stream/dataRateTry/ snapshotDateInfo=today snapshotDate=0
      //second stream

spark-submit --class org.controller.deltaLakeEG.streamUsingRateExample --packages io.delta:delta-core_2.11:0.5.0 --num-executors 1 --executor-memory 1g --driver-cores 1 --driver-memory 1g --deploy-mode client --master yarn /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkPointPath=hdfs://localhost/user/raptor/kafka/stream/checkPoint2RateTry/ outputPath=hdfs://localhost/user/raptor/kafka/stream/dataRateTry/ snapshotDateInfo=before snapshotDate=1
      //third stream

spark-submit --class org.controller.deltaLakeEG.streamUsingRateExample --packages io.delta:delta-core_2.11:0.5.0 --num-executors 1 --executor-memory 1g --driver-cores 1 --driver-memory 1g --deploy-mode client --master yarn /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar checkPointPath=hdfs://localhost/user/raptor/kafka/stream/checkPoint3RateTry/ outputPath=hdfs://localhost/user/raptor/kafka/stream/dataRateTry/ snapshotDateInfo=after snapshotDate=1

//reading it in spark REPL

spark-shell --packages io.delta:delta-core_2.11:0.5.0

val streamDF=spark.readStream.format("delta").option("ignoreChanges","true").option("ignoreDeletes","true").load("/user/raptor/kafka/stream/dataRateTry/").writeStream.outputMode("append").format("console").option("checkpointLocation","/user/raptor/kafka/stream/"+sc.applicationId).start // append will only work on showing data


val streamDF=spark.readStream.format("delta").option("ignoreChanges","true").option("ignoreDeletes","true").load("/user/raptor/kafka/stream/dataRateTry/").groupBy("SnapshotDate").agg(count("stringValue")).writeStream.outputMode("update").format("console").option("checkpointLocation","/user/raptor/kafka/stream/"+sc.applicationId).start // to compute aggregations we need to use update mode

// Accepted output modes are 'append', 'complete', 'update'

      */

    }
}
