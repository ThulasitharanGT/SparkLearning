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
      , (1,1,2,1,27.3)
      , (1,1,2,2,41.9)
      ,(1,1,2,3,32.8)
     , (1,1,3,1,30.8)
      , (1,1,3,2,41.9)
      ,(1,1,3,3,36.7)
    ).toDF("trackId,driverId,lapNo,sectorId,timeTaken".split(","):_*)


    val joinedDF=trackRefDF.join(dataOfLap,Seq("trackId","sectorId"))

    joinedDF.show(false)
    joinedDF.as[lapInPerSector].show(false)

    // val avgSpeed = findSpeedPerSector.toColumn.name("speed")

   Seq(1,2,3).map(x => joinedDF.filter(s"sectorId= ${x}").as[lapInPerSector].
     select( findSpeedPerSector.toColumn.name("speed"))).reduce(_.union(_)).show(false)

  }

}

// take the sector distance from another table
/*

Seq((1,1,2.45)).toSeq("trackId,sectorId,distanceInKM".split(","):_*)

*/
case class lapInPerSector(trackId:Int,driverId:Int,lapNo:Int,sectorId:Int,timeTaken:Double,distanceInKM:Double)
case class lapInterPerSector(trackId:Int,driverId:Int,sectorId:Int,timeTaken:Double,avgSpeed:Double,distanceInKM:Double)
case class lapOutPerSector(trackId:Int,driverId:Int,sectorId:Int,avgTimeTaken:Double,avgSpeed:Double)

case class fastestLap(trackId:Int,driverId:Int,lapId:Int,avgSpeed:Double,timeTaken:Double)

object findSpeedPerSector extends Aggregator[lapInPerSector,lapInterPerSector,lapOutPerSector] {
  var lapSet:Set[Int] = Set.empty[Int]
 /* def this(totalRecords:Int){
    this()
    this.totalRecords=totalRecords
  }*/
  override def zero: lapInterPerSector = {
    println(s"Defined")
    lapInterPerSector(0, 0, 0, 0.0, 0.0, 0.0)
  }

  override def reduce(b: lapInterPerSector, a: lapInPerSector): lapInterPerSector = {
    println(s"reduce lapSet ${lapSet}")
    println(s"reduce lapInter ${b}")
    println(s"reduce lapIn ${a}")
    lapSet+=a.lapNo
    lapInterPerSector(a.trackId, a.driverId, a.sectorId, a.timeTaken + b.timeTaken, 0.0, a.distanceInKM + b.distanceInKM)
  }

  // b1  is always the initialized one
  override def merge(b1: lapInterPerSector, b2: lapInterPerSector): lapInterPerSector = {
    println(s"merge")
    println(s"merge b1 ${b1}")
    println(s"merge b2 ${b2}")
    lapInterPerSector(b2.trackId, b2.driverId, b2.sectorId, b1.timeTaken + b2.timeTaken, 0.0, b1.distanceInKM + b2.distanceInKM)
  }

  override def finish(reduction: lapInterPerSector): lapOutPerSector = {
    println(s"reduction ${reduction}")
    println(s"lapSet ${lapSet}")
    lapOutPerSector(reduction.trackId, reduction.driverId, reduction.sectorId, reduction.timeTaken / lapSet.size, reduction.distanceInKM / ((reduction.timeTaken /60.0) /60.0) ) // to hour
}
  override def bufferEncoder: Encoder[lapInterPerSector] = org.apache.spark.sql.Encoders.product[lapInterPerSector]
    //org.apache.spark.sql.Encoders.bean[lapInter](lapInter.getClass.asInstanceOf[Class[lapInter]])

  override def outputEncoder: Encoder[lapOutPerSector] = org.apache.spark.sql.Encoders.product[lapOutPerSector]
    //org.apache.spark.sql.Encoders.bean[lapOut](lapOut.getClass.asInstanceOf[Class[lapOut]])
}