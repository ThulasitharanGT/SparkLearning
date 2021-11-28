package org.controller.customState

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.util.SparkOpener

object simpleState extends SparkOpener{
  val spark=SparkSessionLoc()
  spark.sparkContext.setLogLevel("ERROR")

  case class tmpMetaStructure(key:String="",valueList:List[tmpStruct]=List.empty)
  case class tmpStruct(key:String,value:String)

  /* just dummy logic, holds the first message in state for a key and releases the same key message's
 in upcoming batch's

 No time out specified. That will be done in next exercise
 */

// returns empty event without any key
  def mapGroupFunction(key:String,valueIterator:Iterator[tmpStruct],groupState:GroupState[tmpMetaStructure])={
    groupState.getOption match {
      case Some(state) =>
        println(s"mapGroupFunction :: Some ${state}")
        state.valueList.isEmpty match {
          case value if value == true =>
            println(s"mapGroupFunction :: Some true")
            groupState.update(tmpMetaStructure(key,valueIterator.toList))
            tmpMetaStructure(key=key)
          case false =>
            println(s"mapGroupFunction :: Some false")
          //  println(s"mapGroupFunction :: Some false valueIterator.toList ${valueIterator.toList}")
            val tmpEvent=valueIterator.toList
            groupState.update(state.copy(valueList = state.valueList ++: tmpEvent))
         //   println(s"mapGroupFunction :: Some false groupState ${groupState.get}")
          //  tmpMetaStructure(key,valueIterator.toList ++: state.valueList)
            groupState.get
        }
      case None =>
        println(s"mapGroupFunction :: None")
        groupState.update(tmpMetaStructure(key,valueIterator.toList))
        tmpMetaStructure(key=key)
    }
  }

  /* just dummy logic, holds the first message in state for a key and releases the same key message's
   in upcoming batch's

   No time out specified. That will be done in next exercise
   */
  // returns empty event with the key
  def mapGroupFunctionUpdated(key:String,valueIterator:Iterator[tmpStruct],groupState:GroupState[tmpMetaStructure])={
    groupState.getOption match {
      case Some(state) =>
        println(s"mapGroupFunctionUpdated :: Some state ${state}")
        state.valueList.isEmpty match {
          case value if value == true =>
            println(s"mapGroupFunctionUpdated :: Some true")
            groupState.update(tmpMetaStructure(key,valueIterator.toList))
            tmpMetaStructure()
          case false =>
            println(s"mapGroupFunctionUpdated :: Some false")
          // println(s"mapGroupFunctionUpdated :: Some ${valueIterator.toList}")
      //      val incomingEvents=valueIterator.toList
        //    groupState.update(state.copy(valueList = state.valueList ++: incomingEvents))
      //      println(s"mapGroupFunctionUpdated :: Some groupState ${groupState}")
            //      tmpMetaStructure(key,incomingEvents ++: state.valueList)
            groupState.update(state.copy(valueList = state.valueList ++: valueIterator.toList))
            groupState.get
        }
      case None =>
        println(s"mapGroupFunctionUpdated :: None")
     //   println(s"mapGroupFunctionUpdated :: None ${valueIterator.toList}")
        groupState.update(tmpMetaStructure(key,valueIterator.toList))
        tmpMetaStructure()
    }
  }

// simple timeout
  def mapGroupFunctionWithTimeOut(key:String,valueIterator:Iterator[tmpStruct],groupState:GroupState[tmpMetaStructure]):tmpMetaStructure={
    if (groupState.hasTimedOut) {
      println("mapGroupFunctionWithTimeOut :: timed out ")
      groupState.remove
      return tmpMetaStructure()
    }
    groupState.getOption match {
      case Some(state) =>
    // handle expired state
        println(s"mapGroupFunctionWithTimeOut :: Some ${state}")
        state.valueList.isEmpty match {
          case value if value == true =>
            println(s"mapGroupFunctionWithTimeOut :: Some true")
            groupState.setTimeoutDuration("5 minutes")
            groupState.update(tmpMetaStructure(key,valueIterator.toList))
            tmpMetaStructure()
          case false =>
            println(s"mapGroupFunctionWithTimeOut :: Some false")
            //  println(s"mapGroupFunction :: Some false valueIterator.toList ${valueIterator.toList}")
            val tmpEvent=valueIterator.toList
            groupState.setTimeoutDuration("5 minutes")
            groupState.update(state.copy(valueList = state.valueList ++: tmpEvent))
            //   println(s"mapGroupFunction :: Some false groupState ${groupState.get}")
            //  tmpMetaStructure(key,valueIterator.toList ++: state.valueList)
            groupState.get
        }
      case None =>
        println(s"mapGroupFunctionWithTimeOut :: None")
        groupState.update(tmpMetaStructure(key,valueIterator.toList))
        groupState.setTimeoutDuration("5 minutes")
        tmpMetaStructure()
    }
  }



  def groupByFun(tempClass:tmpStruct)=tempClass.key

  def main(args:Array[String]):Unit={
    import spark.implicits._
    val inputMap=collection.mutable.Map[String,String]()

    args.map(_.split("=",2)).map(x => inputMap.put(x(0),x(1)))

    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers",inputMap("bootStrapServers"))
      .option("subscribe",inputMap("topic")).load.select(
      org.apache.spark.sql.functions.split(
        org.apache.spark.sql.functions.col("value")
          .cast(org.apache.spark.sql.types.StringType)
          .as("valueCasted"),",").as("valueSplitted"))
      .selectExpr("valueSplitted[0] as key","valueSplitted[1] as value" ).as[tmpStruct]
      .groupByKey(groupByFun).mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mapGroupFunctionWithTimeOut) // check event timeout with watermark
     // .mapGroupsWithState(GroupStateTimeout.NoTimeout)(mapGroupFunction) // curried function
      .withColumn("cool",org.apache.spark.sql.functions.lit("cool"))
      .writeStream.format("console").outputMode("update")
      .option("checkpointLocation",inputMap("checkpointLocation"))
      .option("truncate","false")
      .start

    spark.streams.awaitAnyTermination

      // .as[Encoder[tmpStruct.class]]

  }

  // spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --class org.controller.customState.simpleState --num-executors 1 --executor-memory 1g --driver-memory 1g --driver-cores 1 --executor-cores 1 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar bootStrapServers=localhost:8082,localhost:8083,localhost:8084 topic=tmpTopic checkpointLocation="hdfs://localhost:8020/user/raptor/stream/state1/"
}
