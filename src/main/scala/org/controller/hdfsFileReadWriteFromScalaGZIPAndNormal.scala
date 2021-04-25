package org.controller


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.{OutputStreamWriter, PrintWriter}
import java.util.zip.GZIPOutputStream
import sys.process._
import java.net.URI
import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import org.apache.spark.sql.functions._
import org.util.SparkOpener

object hdfsFileReadWriteFromScalaGZIPAndNormal extends SparkOpener{
  val hdfsDomain="hdfs://localhost:8020"
  val spark=SparkSessionLoc()
  def main(args:Array[String]):Unit={
    val inputString=spark.read.format("csv").option("delimiter","~").option("header","false").option("inferSchema","false").load("file:///home/raptor/IdeaProjects/SparkLearning/Input/kafka/topicLevelCompression/2MB.txt").select(concat(col("_c0"),col("_c1")).as("strTemp")).collect.map(_(0)).mkString(":")
    createTxtFile(inputString,s"${hdfsDomain}/user/raptor/tmp/tmp/tmp.txt")
    createGzipFile(inputString,s"${hdfsDomain}/user/raptor/tmp/tmp/tmp.txt")
    readGzipFile(s"${hdfsDomain}/user/raptor/tmp/tmp/tmp.txt")
    readTxtFile(s"${hdfsDomain}/user/raptor/tmp/tmp/tmp.txt")

  }
  // val inputString=spark.read.format("csv").option("delimiter","~").option("header","false").option("inferSchema","false").load("file:///home/raptor/IdeaProjects/SparkLearning/Input/kafka/topicLevelCompression/2MB.txt").select(concat(col("_c0"),col("_c1")).as("strTemp")).collect.map(_(0)).mkString(":")


  // nongzip file
  def createTxtFile(inputString:String,path:String) = {
    val config = new Configuration
    config.set("fs.defaultFS", hdfsDomain)
    val fs = FileSystem.get(config)
    s"hdfs dfs -mkdir -p ${hdfsDomain}/user/raptor/tmp/tmp/" !
   // val pathTmp = new Path(s"${hdfsDomain}/user/raptor/tmp/tmp/tmp.txt")
   val pathTmp = new Path(s"${path}")
    val fileOutputObj = fs.create(pathTmp)
    val writer = new PrintWriter(fileOutputObj)
    writer.write(inputString)
    writer.close
    fileOutputObj.close
    fs.close
  }

  // gzip file
  def createGzipFile(inputString:String,path:String) = {
    val config = new Configuration
    config.set("fs.defaultFS", hdfsDomain)
    val fs = FileSystem.get(config)
    s"hdfs dfs -mkdir -p ${hdfsDomain}/user/raptor/tmp/gZipFile/" !
    //val pathTmp = new Path(s"${hdfsDomain}/user/raptor/tmp/gZipFile/tmp.txt.gz")
    val pathTmp = new Path(s"${path}")
    val fileOutputObj = fs.create(pathTmp)
    val writer = new OutputStreamWriter(new GZIPOutputStream(fileOutputObj), "UTF-8")
    writer.write(inputString)
    writer.close
    fileOutputObj.close
    fs.close
  }

  // read
  // gzip
  def readGzipFile(path:String) = {
    val fs = FileSystem.get(new URI(hdfsDomain), new Configuration)
    //val pathTmp = new Path(s"${hdfsDomain}/user/raptor/tmp/gZipFile/tmp.txt.gz")
    val pathTmp = new Path(s"${path}")
    val hdfsInputStream = fs.open(pathTmp)
    val inputStream = new GZIPInputStream(hdfsInputStream)
    val bufferedStreamReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
    var fileContent = ""
    var readFlag = true
    while (readFlag)
      bufferedStreamReader.readLine match {
        case value if value != null => fileContent = fileContent + value
        case value if value == null =>
          readFlag = false
      }
    println(fileContent)
    bufferedStreamReader.close
    inputStream.close
    hdfsInputStream.close
    fs.close
  }
  //normal
  def readTxtFile(path:String) = {
    val fs = FileSystem.get(new URI(hdfsDomain), new Configuration)
  //  val pathTmp = new Path(s"${hdfsDomain}/user/raptor/tmp/tmp/tmp.txt")
  val pathTmp = new Path(s"${path}")
    val hdfsInputStream = fs.open(pathTmp)
    val bufferedStreamReader = new BufferedReader(new InputStreamReader(hdfsInputStream))
    var fileContent = ""
    var readFlag = true
    while (readFlag)
      bufferedStreamReader.readLine match {
        case value if value != null => fileContent = fileContent + value
        case value if value == null =>
          readFlag = false
      }
    println(fileContent)
    bufferedStreamReader.close
    hdfsInputStream.close
    fs.close
  }
}
