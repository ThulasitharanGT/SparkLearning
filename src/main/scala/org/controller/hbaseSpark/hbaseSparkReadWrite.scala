package org.controller.hbaseSpark

import org.util.{SparkOpener, readWriteUtil}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.spark.implicits._
import org.apache.spark.sql.SparkSession


object hbaseSparkReadWrite extends SparkOpener{

  /* use spark 2.4.0
  spark-submit --class org.controller.hbaseSpark.hbaseSparkReadWrite --deploy-mode client --master yarn --num-executors 1 --executor-memory 1g --executor-cores 2 --driver-memory 1g --driver-cores 1 SparkLearning-1.0-SNAPSHOT.jar  --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11,com.hortonworks:shc:1.1.1-2.1-s_2.11,org.apache.hbase:hbase-client:1.2.5,org.apache.hbase:hbase-server:1.2.5,org.apache.hbase:hbase-common:1.2.5,org.apache.hbase:hbase-protocol:1.2.5,org.apache.hbase:hbase-hadoop2-compat:1.2.5,org.apache.hbase:hbase-annotations:1.2.5 --repositories https://repository.apache.org/content/repositories/releases
*/

  def main(args: Array[String]): Unit = {

    val spark=SparkSessionLoc("Hbase read")
    def carCatalog = s"""{
        "table":{"namespace":"default", "name":"cardata"},
        "rowkey":"key",
        "columns":{
        "vehicle_id":{"cf":"rowkey", "col":"key", "type":"string"},
        "alloy_wheels":{"cf":"hardware", "col":"alloy_wheels", "type":"string"},
        "anti_Lock_break":{"cf":"hardware", "col":"anti_Lock_break", "type":"string"},
        "electronic_breakforce_distribution":{"cf":"software", "col":"electronic_breakforce_distribution", "type":"string"},
        "terrain_mode":{"cf":"software", "col":"terrain_mode", "type":"string"},
        "traction_control":{"cf":"software", "col":"traction_control", "type":"string"},
        "stability_control":{"cf":"software", "col":"stability_control", "type":"string"},
        "cruize_control":{"cf":"software", "col":"cruize_control", "type":"string"},
        "make":{"cf":"other", "col":"make", "type":"string"},
        "model":{"cf":"other", "col":"model", "type":"string"},
        "variant":{"cf":"other", "col":"variant", "type":"string"}
        }
        }""".stripMargin

    val hbaseDf=readWriteUtil.withCatalog(spark,carCatalog)
    hbaseDf.show(5,false)

  }

}
