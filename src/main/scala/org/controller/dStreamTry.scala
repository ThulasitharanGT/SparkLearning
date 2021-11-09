package org.controller

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds


object dStreamTry {
  def main (args:Array[String]):Unit ={
 //   val conf= new org.apache.spark.SparkConf().setMaster("local[*]").setAppName("tempApp")
  //  val sparkContext= new org.apache.spark.SparkContext(conf)
/*

    val streamingContext= new org.apache.spark.streaming.StreamingContext(sparkContext,Seconds(10))

    streamingContext.checkpoint("hdfs://localhost:8020/user/raptor/streams/checkpointDstream/")
    val dStream1=streamingContext.socketTextStream("localhost",7088 ,StorageLevel.MEMORY_AND_DISK_SER).map(x => (x.split(",",2)(0),x.split(",",2)(1)))

    val dStream2=streamingContext.socketTextStream("localhost",7087 ,StorageLevel.MEMORY_AND_DISK_SER).map(x => (x.split(",",2)(0),x.split(",",2)(1)))

    dStream1.join(dStream2).foreachRDD(rdd => {
    @transient  val sparkSession=org.apache.spark.sql.SparkSession.builder.config(sparkContext.getConf).getOrCreate
       import sparkSession.implicits._
      @transient    val df=rdd.toDF("col1,col2".split(",").toSeq:_*)
      df.show(false)
    })

    dStream1.join(dStream2).print
    streamingContext.start

    streamingContext.awaitTermination
*/

    /*
      dStream1.join(dStream2).foreachRDD(rdd => {
     val sparkSession=org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
       import sparkSession.implicits._
      val df=rdd.toDF("col1,col2".split(",").toSeq:_*)
           df.show(false)
    })

    dStream1.foreachRDD(rdd => {
     val sparkSession=org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
       import sparkSession.implicits._
      val df=rdd.toDF("col1,col2".split(",").toSeq:_*)
           df.show(false)
    })

    dStream1.print
    */
    val spark= org.apache.spark.sql.SparkSession.builder.getOrCreate
    val streamingContext= new org.apache.spark.streaming.StreamingContext(spark.sparkContext,Seconds(10))
    val dStream1=streamingContext.socketTextStream("localhost",7088 ,StorageLevel.MEMORY_AND_DISK_SER).map(x => (x.split(",",2)(0),x.split(",",2)(1)))
    dStream1.print
    streamingContext.start


    dStream1.foreachRDD(rdd => {
      val sparkSession=org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
      import sparkSession.implicits._
      val df=rdd.toDF("col1,col2".split(",").toSeq:_*)
      df.show(false)
    })


    val dStream2=streamingContext.socketTextStream("localhost",7087 ,StorageLevel.MEMORY_AND_DISK_SER).map(x => (x.split(",",2)(0),x.split(",",2)(1)))

// union
    dStream1.union(dStream2).foreachRDD(rdd => {
      val sparkSession=org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
      import sparkSession.implicits._
      val df=rdd.toDF("col1,col2".split(",").toSeq:_*)
      df.show(false)
    })

    // join

    dStream1.join(dStream2).foreachRDD(rdd => {
      val sparkSession=org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
      import sparkSession.implicits._
      val df=rdd.toDF("col1,col2".split(",").toSeq:_*)
      df.show(false)
    })
/*
*   join(otherStream, [numTasks])	When called on two DStreams of (K, V) and (K, W) pairs, return a new DStream of (K, (V, W)) pairs with all pairs of elements for each key.
*
* +----+------+
|col1|col2  |
+----+------+
|s   |{2, 1}|
+----+------+

*
* */

    // left join
    dStream1.leftOuterJoin(dStream2).foreachRDD(
      rdd =>
     /*   rdd.foreachPartition(x =>
       x.toSeq )
       */ {
        val sparkSession = org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
        import sparkSession.implicits._
        rdd.toDF("col1,col2".split(",").toSeq:_*).show(false)
      }
    )

    // right join
    dStream1.rightOuterJoin(dStream2).foreachRDD(
      rdd =>
        /*   rdd.foreachPartition(x =>
          x.toSeq )
          */ {
        val sparkSession = org.apache.spark.sql.SparkSession.builder.config(spark.sparkContext.getConf).getOrCreate
        import sparkSession.implicits._
        rdd.toDF("col1,col2".split(",").toSeq:_*).show(false)
      }
    )


    dStream1.map(x => (x._1,x._2)).groupByKey.join(dStream2).print


    streamingContext.start
    streamingContext.awaitTermination


  }
}
