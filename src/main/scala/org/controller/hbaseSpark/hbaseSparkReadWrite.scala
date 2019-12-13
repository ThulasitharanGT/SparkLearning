package org.controller.hbaseSpark

import org.util.SparkOpener

import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.implicits._
import org.apache.hadoop.hbase.client.{HBaseAdmin, Result}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable


object hbaseSparkReadWrite extends SparkOpener{

  def main(args: Array[String]): Unit = {

  }

}
