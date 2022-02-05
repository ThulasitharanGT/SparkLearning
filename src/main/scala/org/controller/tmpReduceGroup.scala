package org.controller

import org.util.SparkOpener

object tmpReduceGroup extends SparkOpener{

  val spark= SparkSessionLoc()
  import spark.implicits._

  case class tmpClass(col1:String,col2:String)
  def main(args:Array[String]):Unit ={

    val df=spark.read.format("csv").option("header","").option("inferSchema","").option("delimiter","").load.as[tmpClass]
    val tmp=df.reduce((x,y)=> x.col1 == y.col2 match {case true => x case false => y})

    val collectionMap=collection.mutable.Map[String,Int]("Apple"->2,"apple"->3,"banana"->1,"Banana"->6,"citrus"->8,"Citrus"->4)

    collectionMap.sameElements(collection.mutable.Map[String,Int]("s"->9))
    collectionMap.scanLeft(0)((x,y)=> y._2 >1 match {case true => x case false => y._2})

    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

    dbutils.fs.cp("","",true)

    import org.apache.spark.sql.functions.{col}
    import org.apache.spark.sql.types.{StringType}
    import org.apache.spark.sql.streaming.Trigger.Once

    spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:8081,localhost:8082,localhost:8083").option("subscribe","temp.topic").option("maxOffsetsPerTrigger","10").load.select(col("value").cast(StringType).as("value"),col("key").cast(StringType).as("key")).writeStream.format("console").option("truncate","false").option("numRows","99999999").trigger(Once).start

  }





 }
