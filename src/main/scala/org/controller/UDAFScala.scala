package org.controller

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
// spark-submit --class org.controller.UDAFScala --driver-memory 512m --driver-cores 2 /home/raptor/IdeaProjects/SparkLearning/build/libs/SparkLearning-1.0-SNAPSHOT.jar

object UDAFScala {

  def main(args:Array[String]):Unit ={

    val spark=org.apache.spark.sql.SparkSession.builder.master("local[*]").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

   val trackRefDF= Seq((1,1,2.45)
     ,(1,2,3.85)
     ,(1,3,1.98)
   ).toDF("trackId,sectorId,distanceInKM".split(","):_*)

    val dataOfLap=Seq((1,1,1,1,29.8)
      , (1,1,1,2,42.2)
      ,(1,1,1,3,35.6)
    ).toDF("trackId,driverId,lapNo,sectorId,timeTaken".split(","):_*)


    val joinedDF=trackRefDF.join(dataOfLap,Seq("trackId","sectorId"))
    joinedDF.show(false)
    joinedDF.as[lapIn].show(false)

    val avgSpeed = findSpeedPerSector.toColumn.name("speed")

    joinedDF.as[lapIn].select(avgSpeed).show(false)


  }

}

// take the sector distance from another table
/*

Seq((1,1,2.45)).toSeq("trackId,sectorId,distanceInKM".split(","):_*)

*/
case class lapIn(trackId:Int,driverId:Int,lapNo:Int,sectorId:Int,timeTaken:Double,distanceInKM:Double)
case class lapInter(trackId:Int,driverId:Int,lapNo:Int,sectorId:Int,timeTaken:Double,avgSpeed:Double,distanceInKM:Double)
case class lapOut(trackId:Int,driverId:Int,sectorId:Int,avgTimeTaken:Double,avgSpeed:Double)

case class fastestLap(trackId:Int,driverId:Int,lapId:Int,avgSpeed:Double,timeTaken:Double)

object findSpeedPerSector extends Aggregator[lapIn,lapInter,lapOut] {
  var totalRecords=0
  override def zero: lapInter = {
    println(s"Defined")
    lapInter(0,0,0,0,0.0,0.0,0.0)
  }

  override def reduce(b: lapInter, a: lapIn): lapInter = {
    println(s"reduce ${totalRecords}")
    println(s"reduce lapInter ${b}")
    println(s"reduce lapIn ${a}")

    totalRecords+=1
    lapInter(a.trackId,a.driverId,a.lapNo,a.sectorId,a.timeTaken+b.timeTaken,0.0,a.distanceInKM+b.distanceInKM)
  }

  override def merge(b1: lapInter, b2: lapInter): lapInter = {
    println(s"merge")
    println(s"merge b1 ${b1}")
    println(s"merge b2 ${b2}")
    lapInter(b1.trackId,b1.driverId,b1.lapNo,b1.sectorId,b1.timeTaken+b2.timeTaken,0.0,b1.distanceInKM+b2.distanceInKM)
  }

  override def finish(reduction: lapInter): lapOut =
    lapOut(reduction.trackId,reduction.driverId,reduction.sectorId,reduction.timeTaken /totalRecords, (reduction.distanceInKM * reduction.timeTaken)/totalRecords)

  override def bufferEncoder: Encoder[lapInter] = org.apache.spark.sql.Encoders.product[lapInter]
    //org.apache.spark.sql.Encoders.bean[lapInter](lapInter.getClass.asInstanceOf[Class[lapInter]])

  override def outputEncoder: Encoder[lapOut] = org.apache.spark.sql.Encoders.product[lapOut]
    //org.apache.spark.sql.Encoders.bean[lapOut](lapOut.getClass.asInstanceOf[Class[lapOut]])
}