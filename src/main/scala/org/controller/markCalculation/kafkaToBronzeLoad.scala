package org.controller.markCalculation

import org.controller.markCalculation.marksCalculationUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types._
import org.controller.markCalculation.marksCalculationConstant._


object kafkaToBronzeLoad {

  def main(args:Array[String]):Unit = {
    val spark=getSparkSession()
    val sc=spark.sparkContext
    sc.setLogLevel("ERROR")

    val inputMap= inputArrayToMap(args)
    inputMap foreach println
    /*  kafkaSubscribeAssignDecider=kafkaSubscribe
        kafkaSubscribe=topicTmp
        kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083
        SAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/"
        CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/"
        bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/"
        bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/"
        SACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/"
        CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/"
        consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"
kafkaStartingOffsetsArg=latest
--master yarn --deploy-mode cluster
readStreamFormat=kafka

spark-submit --master local --class org.controller.markCalculation.kafkaToBronzeLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,io.delta:delta-core_2.12:1.1.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 SAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/" CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/" bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/" bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/" SACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/" CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/" readStreamFormat=kafka kafkaStartingOffsets=latest consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"
    sh $1 , $2
    spark-submit --master local --class org.controller.markCalculation.kafkaToBronzeLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/" bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/" readStreamFormat=kafka kafkaStartingOffsets=latest consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"

300

use spark 3.0.0 , 3..1.x has issues with delta lake
    spark-submit --master local --class org.controller.markCalculation.kafkaToBronzeLoad --num-executors 3 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 3 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 SAPath="hdfs://localhost:8020/user/raptor/persist/marks/SA/" CAPath="hdfs://localhost:8020/user/raptor/persist/marks/CA/" bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/" bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/" SACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/SummativeAssessment/" CACheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/CumulativeAssessment/" readStreamFormat=kafka kafkaStartingOffsets=latest consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"


latest
spark-submit --master local --class org.controller.markCalculation.kafkaToBronzeLoad --num-executors 2 --executor-memory 512m --executor-cores 2 --driver-memory 512m --driver-cores 2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,io.delta:delta-core_2.12:0.8.0,com.fasterxml.jackson.module:jackson-module-scala_2.12:2.10.0,com.fasterxml.jackson.core:jackson-databind:2.10.0 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar kafkaSubscribeAssignDecider=kafkaSubscribe kafkaSubscribe=topicTmp kafkaBootstrapServer=localhost:8081,localhost:8082,localhost:8083 bronzePath="hdfs://localhost:8020/user/raptor/persist/marks/bronze/" bronzeCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/bronze/" readStreamFormat=kafka kafkaStartingOffsets=latest consoleCheckpoint="hdfs://localhost:8020/user/raptor/stream/checkpoint/consoleCheckpoint1/"

// this just persists whatever data which comes in
     */

    val readStreamDF=getReadStreamDF(spark,inputMap).select(col("value").cast(StringType)).select(from_json(col("value"),wrapperSchema).as("schemaExploded")).select(col("schemaExploded.*"))

    readStreamDF.writeStream.format("console").outputMode("append")
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) => {
        //  foreachBatchFun(df, batchID, inputMap, "bronze", spark)
        inputMap.put(writeStreamFormat,deltaStreamFormat)
        inputMap.put(deltaMergeOverwriteDecider,deltaMerge)
        inputMap.put(deltaMerge,"false")
        inputMap.put(pathArg,inputMap("bronzePath"))
        foreachBatchFunBronze(spark,df.withColumn("batchID",lit(batchID)),inputMap)
      })
      .option("checkpointLocation",inputMap("bronzeCheckpoint"))
      .start

    /*
    val summativeAssessmentStream=readStreamDF.filter(s"messageType='${summativeAssessment}'")

    val cumulativeAssessmentStream=readStreamDF.filter(s"messageType='${cumulativeAssessment}'")


    readStreamDF.writeStream.format("console").outputMode("append").option("truncate","false").option("numRows","99999999").option("checkpointLocation",inputMap("consoleCheckpoint")).start

       innerMsgParser(cumulativeAssessmentStream).writeStream.format("console")
       .outputMode("append")
       .option("checkpointLocation",inputMap("CACheckpoint"))
       .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) =>
         //  foreachBatchFun(df,batchID,inputMap,cumulativeAssessment,spark)
           foreachBatchFunSA(spark,df,inputMap))
       .start

       innerMsgParser(summativeAssessmentStream).writeStream.format("console")
      .outputMode("append")
      .option("checkpointLocation",inputMap("CACheckpoint"))
      .foreachBatch((df:org.apache.spark.sql.DataFrame,batchID:Long) =>
        foreachBatchFunCA(spark,df,inputMap))
        //foreachBatchFun(df,batchID,inputMap,summativeAssessment,spark) )
      .start

*/
    spark.streams.awaitAnyTermination


  }

  val foreachBatchFun:(org.apache.spark.sql.DataFrame,Long,collection.mutable.Map[String,String],String,org.apache.spark.sql.SparkSession) =>Unit=
    (df:org.apache.spark.sql.DataFrame,batchID:Long,tmpMap:collection.mutable.Map[String,String],controlString:String,spark:org.apache.spark.sql.SparkSession)=>controlString match {
      case value if value == "bronze" =>
        println("foreachBatchFun bronze")
        tmpMap.put(writeStreamFormat,deltaStreamFormat)
        tmpMap.put(deltaMergeOverwriteDecider,deltaMerge)
        tmpMap.put(deltaMerge,"false")
        tmpMap.put(pathArg,tmpMap("bronzePath"))
        foreachBatchFunBronze(spark,df.withColumn("batchID",lit(batchID)),tmpMap)
    /*  case value if value == summativeAssessment =>
        println("foreachBatchFun summativeAssessment")
        tmpMap.put(writeStreamFormat,deltaStreamFormat)
        tmpMap.put(deltaMergeOverwriteDecider,deltaMerge)
        tmpMap.put(deltaMerge,"false")
      //  tmpMap.put(pathArg,tmpMap("SAPath")) // changing path for same key in the map object which is being pushed across is an issue
        foreachBatchFunSA(spark,df,tmpMap)
      case value if value == cumulativeAssessment =>
        println("foreachBatchFun cumulativeAssessment")
        tmpMap.put(writeStreamFormat,deltaStreamFormat)
        tmpMap.put(deltaMergeOverwriteDecider,deltaMerge)
        tmpMap.put(deltaMerge,"false")
     //   tmpMap.put(pathArg,tmpMap("CAPath"))
        foreachBatchFunCA(spark,df,tmpMap)*/
    }


  def foreachBatchFunBronze(spark:org.apache.spark.sql.SparkSession,df:org.apache.spark.sql.DataFrame,map:collection.mutable.Map[String,String])= dfWriterStream(spark,df,map)

}
