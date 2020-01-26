package org.controller.deltaLakeEG

import org.util.SparkOpener
import io.delta.tables.DeltaTable
import sys.process._
object pushingStreamedDataToDeltaBronze extends SparkOpener{
  val spark=SparkSessionLoc("writing Kafka dump to delta table")

  def main(args: Array[String]): Unit = {
    val inputMap:collection.mutable.Map[String,String]=collection.mutable.Map[String,String]()
    for(arg <- args)
    {
      val keyPart=arg.split("=",2)(0)
      val valPart=arg.split("=",2)(1)
      inputMap.put(keyPart,valPart)
    }

    val basePath=inputMap("basePath")//hdfs://localhost/user/raptor/kafka/temp/output/kafkaDeltaTableDump/
    val sourceTable=inputMap("sourceTable")//carSensorStreamedData
    val datePartitionValue=inputMap("datePartitionValue")//2019-12-20
    val bronzeTable=inputMap("bronzeTable")//carSensorBronze
    val dfSource= spark.read.option("basePath",basePath).load(basePath+sourceTable+"/date="+datePartitionValue+"/").selectExpr("CAST (offset as long) as offset", "CAST( timestamp as timestamp ) as timestamp","CAST(value as string ) as value","CAST(timestampType as int) as timestampType","CAST(date as string ) as date", "CAST(topic as string ) as topic","CAST(partition as int ) as partition", "CAST(key as string  ) as key")
    val overWriteOrAppend=inputMap("overWriteOrAppend")//overwrite or append or overwriteFully
    val result="hdfs dfs -ls "+basePath+bronzeTable
/*
"CAST (offset as long) as offset", "CAST( timestamp as timestamp ) as timestamp","CAST(value as string ) as value","CAST(timestampType as int) as timestampType","CAST(date as date ) as date", "CAST(topic as string ) as topic","CAST(partition as int ) as partition", "CAST(key as string  ) as key"
 */
    result! match
    {
      case 0 => overWriteOrAppend match { case "overwrite"=> dfSource.write.mode("overwrite").format("delta").option("spark.sql.sources.partitionOverwriteMode","dynamic").partitionBy("date","topic","partition","key").save(basePath+bronzeTable); case "overwriteFully" =>dfSource.write.mode("overwrite").format("delta").partitionBy("date","topic","partition","key").save(basePath+bronzeTable); case "append" =>dfSource.write.mode("append").format("delta").partitionBy("date","topic","partition","key").save(basePath+bronzeTable); case _ => println("Invalid option only options available are : overwrite , append and overwriteFully") }
      case _ =>{ dfSource.write.mode("overwrite").partitionBy("date","topic","partition","key").save(basePath+bronzeTable);DeltaTable.convertToDelta(spark,"parquet.`"+basePath+bronzeTable+"`","date string,topic string, partition integer, key string")}
    }
    spark.close
  }

}

/*
cd /home/raptor/IdeaProjects/SparkLearning/build/libs/

spark-submit --class org.controller.deltaLakeEG.pushingStreamedDataToDeltaBronze  --num-executors 1 --executor-cores 2 --executor-memory 1g --driver-memory 1g --driver-cores 1 --packages io.delta:delta-core_2.11:0.5.0 SparkLearning-1.0-SNAPSHOT.jar basePath=hdfs://localhost/user/raptor/kafka/temp/output/kafkaDeltaTableDump/ sourceTable=carSensorStreamedData datePartitionValue=2019-12-20 bronzeTable=carSensorBronze overWriteOrAppend=append


 */
